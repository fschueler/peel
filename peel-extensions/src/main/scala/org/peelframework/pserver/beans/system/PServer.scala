/**
 * Copyright (C) 2014 TU Berlin (peel@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.peelframework.pserver.beans.system

import com.samskivert.mustache.Mustache
import org.peelframework.core.beans.system.Lifespan._
import org.peelframework.core.beans.system.{SetUpTimeoutException, LogCollection, System}
import org.peelframework.core.config.{Model, SystemConfig}
import org.peelframework.core.util.shell

import scala.util.matching.Regex

/** PServer System implementation
  *
  * @param version Version of the system (e.g. "7.1")
  * @param configKey The system configuration resides under `system.\${configKey}`.
  * @param lifespan `Lifespan` of the system
  * @param dependencies Set of dependencies that this system needs
  * @param mc The moustache compiler to compile the templates that are used to generate property files for the system
  */
class PServer(
               version      : String,
               configKey    : String,
               lifespan     : Lifespan,
               dependencies : Set[System] = Set(),
               mc           : Mustache.Compiler) extends System("pserver", version, configKey, lifespan, dependencies, mc)
with LogCollection {


  // ---------------------------------------------------
  // LogCollection.
  // ---------------------------------------------------

  /** The patterns of the log files to watch. */
  override protected def logFilePatterns(): Seq[Regex] = {
    List("pserver-.+\\.log".r, "pserver-.+\\.out".r)
  }

  // ---------------------------------------------------
  // System.
  // ---------------------------------------------------

  override def configuration() = SystemConfig(config, {
    val conf = config.getString("system.pserver.path.config")
    List(
      SystemConfig.Entry[Model.Hosts]("system.pserver.config.slaves", s"$conf/nodes", templatePath("conf/hosts"), mc),
      SystemConfig.Entry[Model.Yaml]("system.pserver.config.yaml", s"$conf/pserver-conf.yaml", templatePath("conf/pserver-conf.yaml"), mc),
      SystemConfig.Entry[Model.Yaml]("system.pserver.config.log4j", s"$conf/log4j.properties", templatePath("conf/log4j.properties"), mc)
    )
  })

  /** Starts up the system and polls to check whether everything is up.
    *
    * @throws SetUpTimeoutException If the system was not brought after {startup.pollingCounter} times {startup.pollingInterval} milliseconds.
    */
  override protected def start(): Unit = {
    val user = config.getString("system.pserver.user")
    val logDir = config.getString("system.pserver.path.log")

    var failedStartUpAttempts = 0
    while (!isUp) {
      try {
        val totl = config.getStringList(s"system.$configKey.config.slaves").size()
        val init = 0
        // TODO get the output of the command and return better error message
        shell ! s"${config.getString("system.pserver.path.home")}/bin/start-cluster.sh"
        logger.info(s"Waiting for nodes to connect")

        var curr = init
        var cntr = config.getInt("system.pserver.startup.polling.counter")
        while (curr - init < totl) {
          logger.info(s"Connected ${curr - init} from $totl nodes")
          // wait a bit
          Thread.sleep(config.getInt("system.pserver.startup.polling.interval"))
          // get new values (run status check on all nodes)
          shell ! s"${config.getString("system.pserver.path.home")}/bin/status.sh"
          // get number of running nodes
          curr = Integer.parseInt((shell !! s"""cat $logDir/status.txt | wc -l""").trim())
          // timeout if counter goes below zero
          cntr = cntr - 1
          if (cntr < 0) throw new SetUpTimeoutException(s"Cannot start system '$toString'; node connection timeout at system ")
        }
        isUp = true
      } catch {
        case e: SetUpTimeoutException =>
          failedStartUpAttempts = failedStartUpAttempts + 1
          if (failedStartUpAttempts < config.getInt("system.pserver.startup.max.attempts")) {
            shell ! s"${config.getString("system.pserver.path.home")}/bin/stop-cluster.sh"
            logger.info(s"Could not bring system '$toString' up in time, trying again...")
          } else {
            throw e
          }
      }
    }
  }

  /** Stops the system. */
  override protected def stop(): Unit = {
    shell ! s"${config.getString("system.pserver.path.home")}/bin/stop-cluster.sh"
    isUp = false
  }

  /** Checks whether a process   for this system is already running.
    *
    * This is different from the value of `isUp`, as a system can be running, but not yet up and operational (i.e. if
    * not all worker nodes of a distributed have connected).
    *
    * @return True if a system process for this system exists.
    */
  override def isRunning: Boolean = {
    Integer.parseInt((shell !! s"""cat ${config.getString("system.pserver.path.log")}/status.txt | wc -l""").trim()) > 0
  }


}

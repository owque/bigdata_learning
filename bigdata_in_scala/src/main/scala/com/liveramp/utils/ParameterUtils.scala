package com.liveramp.utils

import org.slf4j.Logger
import org.slf4j.LoggerFactory._

object ParameterUtils {
  val log: Logger = getLogger("=============")
  var handle_parameter_i: Int = 0
  var parameter: String = ""
  var numTasks: Int = 2
  var src: String = ""
  var dest: String = ""
  var ignoreErrors = false
  var threadWhileListFiles = 1

  def getNumTasks(): Int = {
    numTasks
  }

  def getSource(): String = {
    src
  }

  def getDestination(): String = {
    dest
  }

  def getIgnoreErrors(): Boolean = {
    ignoreErrors
  }

  def getThreadWhileListFiles(): Int = {
    threadWhileListFiles
  }

  def fun_get_next_parameter(parameter_array: Array[String]): Unit = {
    log.info("handle parameter before index " + handle_parameter_i)
    if (handle_parameter_i <= 0) {

      parameter = parameter_array(0);
      handle_parameter_i = 1;
    } else if (handle_parameter_i == parameter_array.length) {
      parameter = null;
      handle_parameter_i = -1;
    }
    else {
      parameter = parameter_array(handle_parameter_i);
      handle_parameter_i = handle_parameter_i + 1;
    }
    log.info("handle parameter after index " + handle_parameter_i)

  }

  def handle_parameters(parameter_array: Array[String]): Unit = {
    log.info("handle parameters... ")
    fun_get_next_parameter(parameter_array)
    while (parameter != null) {
      log.info("parameter " + parameter)
      if (parameter == "--numTasks") {

        fun_get_next_parameter(parameter_array)
        numTasks = parameter.toInt;
        log.info("get number of tasks " + numTasks)
      } else if (parameter == "--source") {
        fun_get_next_parameter(parameter_array)
        src = parameter
        log.info("get source to copy: " + src)
      } else if (parameter == "--destination") {
        fun_get_next_parameter(parameter_array)
        dest = parameter
        log.info("get destination to copy: " + dest)
      } else if (parameter == "--ignoreErrors") {
        ignoreErrors = true
        log.info("get parameter ignoreErrors ")
      } else if (parameter == "--thread") {
        fun_get_next_parameter(parameter_array)
        threadWhileListFiles = parameter.toInt
        log.info("get parameter thread while list files :" + threadWhileListFiles)

      }
      fun_get_next_parameter(parameter_array)
    }

  }
}

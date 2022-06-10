package com.liveramp.v2

import java.net.URI

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.liveramp.utils.HDFS
import com.liveramp.v2.model.BaseArgument
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger
import org.slf4j.LoggerFactory._

/**
 * BaseLauncher
 *
 * @author owque
 * @Date 2020/11/16
 */
trait BaseLauncher {
  val log: Logger = getLogger("=============")
  var runMode = ""
  var argumentFile = ""
  var stateOutputUri = ""
  var sparkSession: SparkSession = _

  def init[T <: BaseArgument](args: Array[String], argClass: Predef.Class[T]): T = {
    parseArgs(args.toList)
    val conf: SparkConf = runMode match {
      case "yarn-cluster" => new SparkConf().setAppName(this.getClass.getName)
      case _ =>
        new SparkConf()
          .setMaster("local[2]")
          .setAppName(this.getClass.getName)
    }
    sparkSession = SparkSession.builder().appName(this.getClass.getName).config(conf).getOrCreate()
    if (StringUtils.isBlank(argumentFile)) {
      throw new IllegalArgumentException("Argument file is empty.")
    }
    log.info("runMode={}, argumentFile = {}", runMode, argumentFile: Any)
    val argsJsonStr = new HDFS(new URI(argumentFile)).read(argumentFile)
    log.info("Get info from argument file: {}", argsJsonStr)
    val argObj = parserJsonArguments(argsJsonStr, argClass)
    stateOutputUri = argObj.stateOutputUri
    argObj.validate()
    argObj
  }

  private def parseArgs(args: List[String]): Unit = args match {
    case "--run_mode" :: value :: tail =>
      runMode = value
      parseArgs(tail)
    case "--argument_file" :: value :: tail =>
      argumentFile = value
      parseArgs(tail)
    case Nil =>
    case _ :: _ :: tail => parseArgs(tail)

    case _ =>
  }

  private def parserJsonArguments[T](argJson: String, t: Predef.Class[T]): T = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.readValue(argJson, t)
  }
}

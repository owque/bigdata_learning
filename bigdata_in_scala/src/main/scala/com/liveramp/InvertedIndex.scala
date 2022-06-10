package com.liveramp

import com.liveramp.v2.BaseLauncher
import com.liveramp.v2.model.BaseArgument
import org.apache.spark.rdd.RDD

/**
 * @author owque
 * @date 2021/7/26
 */
object InvertedIndex extends BaseLauncher {

  def main(args: Array[String]): Unit = {
    log.info("Start with args:{}", args.toList)
    try {
      argumentFile = "./data/argument_files/argument1.json"
      val argument = init(args, classOf[BaseArgument])
      val srcRDD: RDD[(String, String)] = sparkSession.sparkContext.wholeTextFiles(argument.inputFilePath)
      srcRDD.flatMap(r=>{
        val filePath = r._1
        val filename = filePath.substring(filePath.lastIndexOf("/")+1)
        r._2.split(" ").map(x=>((x,filename),1))
      }).reduceByKey(_+_)
        .map(r=>{
          val word = r._1._1
          val filename = r._1._2
          val time = r._2
          (word,List((filename,time)))
        }).reduceByKey(_++_)
        .map(x=>{
          x._1+":"+x._2.mkString(",")
        })
        .saveAsTextFile(argument.outputPath)

    } catch {
      case e: Exception =>
        log.error(e.getMessage,e)
        throw e
    } finally {
      sparkSession.stop()
    }
  }


}

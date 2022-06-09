package com.liveramp

import java.net.URI

import com.liveramp.objects.{ConfigSerializableDeser, CopyDefinitionWithDependencies, FileSystemObjectCacher}
import com.liveramp.utils._
import org.apache.hadoop.fs._
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object SparkDistCP {

  type KeyedCopyDefinition = (URI, CopyDefinitionWithDependencies)


  def main(args: Array[String]): Unit = {
    ParameterUtils.handle_parameters(args)
    val sparkSession = SparkSession.builder().getOrCreate()


    val src = new Path(ParameterUtils.getSource())
    val dest = new Path(ParameterUtils.getDestination())
    run(sparkSession, src, dest)

  }




  def run(sparkSession: SparkSession, sourcePath: Path, destinationPath: Path): Unit = {
    val qualifiedSourcePath = PathUtils.pathToQualifiedPath(sparkSession.sparkContext.hadoopConfiguration, sourcePath)
    val qualifiedDestinationPath = PathUtils.pathToQualifiedPath(sparkSession.sparkContext.hadoopConfiguration, destinationPath)
    val sourceRDD = HandleFileUtils.getFilesFromSourceHadoop(sparkSession.sparkContext, qualifiedSourcePath.toUri, qualifiedDestinationPath.toUri,ParameterUtils.getNumTasks())
    LoggingUtils.log("Info","source rdd "+sourceRDD.map(x => x.toString).reduce((x, y) => x + "," + y))
    val destinationRDD = HandleFileUtils.getFilesFromDestinationHadoop(sparkSession.sparkContext, qualifiedDestinationPath)
    LoggingUtils.log("Info","destination rdd "+destinationRDD.map(x => x.toString).reduce((x,y) => x + "," + y))
    LoggingUtils.log("Info","SparkDistCP tasks number : \n" + sourceRDD.partitions.length)
    val joined = sourceRDD.fullOuterJoin(destinationRDD)
    LoggingUtils.log("Info","joined rdd "+joined.map(x => x.toString).reduce((x,y) => x + "," + y))
    val toCopy = joined.collect { case (_, (Some(s), _)) => s }


    val copyResult: RDD[String] = doCopy(toCopy)

    copyResult.foreach(_ => ())
    LoggingUtils.log("Info","Success to copy files")



  }


  private[squadron] def doCopy(sourceRDD: RDD[CopyDefinitionWithDependencies]): RDD[String] = {


    val serConfig = new ConfigSerializableDeser(sourceRDD.sparkContext.hadoopConfiguration)

    LoggingUtils.log("Info","do copy START ")
    var distcpresult = sourceRDD
      .mapPartitions {
        iterator =>
          val hadoopConfiguration = serConfig.get()
          val attemptID = TaskContext.get().taskAttemptId()
          val fsCache = new FileSystemObjectCacher(hadoopConfiguration)

          iterator
            .flatMap(_.getAllCopyDefinitions)
            .collectMapWithEmptyCollection((d, z) => z.contains(d),
              d => {
                val r = CopyUtils.handleCopy(fsCache.getOrCreate(d.source.uri), fsCache.getOrCreate(d.destination), d, attemptID)
                r
              }
            )
      }
    LoggingUtils.log("Info","do copy END ")
    distcpresult
  }

  private[squadron] implicit class DistCPIteratorImplicit[B](iterator: Iterator[B]) {

    def collectMapWithEmptyCollection(skip: (B, Set[B]) => Boolean, action: B => String): Iterator[String] = {

      iterator.scanLeft((Set.empty[B], None: Option[String])) {
        case ((z, _), d) if skip(d, z) => (z, None)
        case ((z, _), d) =>
          (z + d, Some(action(d)))
      }
        .collect { case (_, Some(r)) => r }

    }

  }



}
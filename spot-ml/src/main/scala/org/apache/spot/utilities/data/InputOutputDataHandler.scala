package org.apache.spot.utilities.data

import org.apache.log4j.Logger
import org.apache.hadoop.fs.{LocatedFileStatus, Path, RemoteIterator, FileUtil => fileUtil}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}


/**
  * Created by rabarona on 12/12/16.
  */
object InputOutputDataHandler {

  def getInputDataFrame(sqlContext: SQLContext, inputPath: String, logger: Logger): Option[DataFrame] ={
    try {
      logger.info("Loading data from: " + inputPath)
      Some(sqlContext.read.parquet(inputPath))
    } catch {
      case _ => {
        logger.error(s"Couldn't read data from location $inputPath, please verify it's a valid location and that " +
          s"contains parquet files with a given schema and try again.")
        None
      }
    }
  }

  def uploadResultsFile(sparkContext: SparkContext, hdfsScoredConnect: String, analysis: String, logger: Logger) {
    val hadoopConfiguration = sparkContext.hadoopConfiguration
    val fileSystem = org.apache.hadoop.fs.FileSystem.get(hadoopConfiguration)

    val exists = fileSystem.exists(new org.apache.hadoop.fs.Path(hdfsScoredConnect))

    if(exists){
      val srcDir = new Path(hdfsScoredConnect)
      val dstFile = new Path(hdfsScoredConnect+"/"+analysis+"_results.csv")
      fileUtil.copyMerge(fileSystem,srcDir, fileSystem, dstFile, false, hadoopConfiguration, "")

      val files: RemoteIterator[LocatedFileStatus] = fileSystem.listFiles(srcDir, false)
      while (files.hasNext){
        val filePath = files.next().getPath()
        if(filePath.toString.contains("part-")){
          fileSystem.delete(filePath, false)
        }
      }
    }
    else logger.info(s"Couldn't find results in $hdfsScoredConnect." +
        s"Please check previous logs to see if there were errors.")
   }

}

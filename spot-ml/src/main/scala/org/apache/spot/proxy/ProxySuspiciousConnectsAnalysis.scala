package org.apache.spot.proxy

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.SQLContext
import org.apache.spot.SuspiciousConnectsArgumentParser.SuspiciousConnectsConfig
import org.apache.spot.proxy.ProxySchema._
import org.apache.spot.utilities.data.validation.{InvalidDataHandler => dataValidation}

/**
  * Run suspicious connections analysis on proxy data.
  */
object ProxySuspiciousConnectsAnalysis {

  /**
    * Run suspicious connections analysis on proxy data.
    *
    * @param config       SuspicionConnectsConfig object, contains runtime parameters from CLI.
    * @param sparkContext Apache Spark context.
    * @param sqlContext   Spark SQL context.
    * @param logger       Logs execution progress, information and errors for user.
    */
  def run(config: SuspiciousConnectsConfig, sparkContext: SparkContext, sqlContext: SQLContext, logger: Logger) = {

    logger.info("Starting proxy suspicious connects analysis.")

    logger.info("Loading data from: " + config.inputPath)

    val rawDataDF = sqlContext.read.parquet(config.inputPath)
      .filter(InputFilter)
      .select(InSchema:_*)
      .na.fill("-", Seq(UserAgent))
      .na.fill("-", Seq(ResponseContentType))

    logger.info("Training the model")
    val model =
      ProxySuspiciousConnectsModel.trainNewModel(sparkContext, sqlContext, logger, config, rawDataDF)

    logger.info("Scoring")
    val scoredDF = model.score(sparkContext, rawDataDF)

    // take the maxResults least probable events of probability below the threshold and sort

    val filteredDF = scoredDF
      .filter(Score +  " <= " + config.threshold + " AND " + Score + " > -1 ")
    val mostSuspiciousDF = filteredDF.orderBy(Score).limit(config.maxResults)

    val outputDF = mostSuspiciousDF.select(OutSchema:_*)

    logger.info("Proxy suspicious connects analysis completed")
    logger.info("Saving results to: " + config.hdfsScoredConnect)
    outputDF.map(_.mkString(config.outputDelimiter)).saveAsTextFile(config.hdfsScoredConnect)

    val invalidRecords = sqlContext.read.parquet(config.inputPath)
      .filter(InvalidRecordsFilter)
      .select(InSchema:_*)
    dataValidation.showAndSaveInvalidRecords(invalidRecords, config.hdfsScoredConnect, logger)

    val corruptRecords = scoredDF.filter(Score + " = -1")
    dataValidation.showAndSaveCorruptRecords(corruptRecords, config.hdfsScoredConnect, logger)
  }


  val InSchema = StructType(
    List(DateField,
      TimeField,
      ClientIPField,
      HostField,
      ReqMethodField,
      UserAgentField,
      ResponseContentTypeField,
      DurationField,
      UserNameField,
      WebCatField,
      RefererField,
      RespCodeField,
      URIPortField,
      URIPathField,
      URIQueryField,
      ServerIPField,
      SCBytesField,
      CSBytesField,
      FullURIField)).fieldNames.map(col)

  val OutSchema = StructType(
    List(DateField,
      TimeField,
      ClientIPField,
      HostField,
      ReqMethodField,
      UserAgentField,
      ResponseContentTypeField,
      DurationField,
      UserNameField,
      WebCatField,
      RefererField,
      RespCodeField,
      URIPortField,
      URIPathField,
      URIQueryField,
      ServerIPField,
      SCBytesField,
      CSBytesField,
      FullURIField,
      WordField,
      ScoreField)).fieldNames.map(col)

  val InputFilter = s"$Date IS NOT NULL AND $Time  IS NOT NULL AND $ClientIP IS NOT NULL AND " +
    s"$Host IS NOT NULL AND $FullURI IS NOT NULL"

  val InvalidRecordsFilter = s"$Date IS NULL OR $Time  IS NULL OR $ClientIP IS NULL OR " +
    s"$Host IS NULL OR $FullURI IS NULL"
}
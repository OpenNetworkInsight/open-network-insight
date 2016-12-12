package org.apache.spot.dns

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spot.SuspiciousConnectsArgumentParser.SuspiciousConnectsConfig
import org.apache.spot.dns.DNSSchema._
import org.apache.spot.dns.model.DNSSuspiciousConnectsModel
import org.apache.log4j.Logger
import org.apache.spot.dns.model.DNSSuspiciousConnectsModel.ModelSchema
import org.apache.spot.proxy.ProxySchema.Score
import org.apache.spot.utilities.data.validation.{InvalidDataHandler => dataValidation}

/**
  * The suspicious connections analysis of DNS log data develops a probabilistic model the DNS queries
  * made by each client IP and flags
  */

object DNSSuspiciousConnectsAnalysis {

  /**
    * Run suspicious connections analysis on DNS log data.
    *
    * @param config Object encapsulating runtime parameters and CLI options.
    * @param sparkContext
    * @param sqlContext
    * @param logger
    */
  def run(config: SuspiciousConnectsConfig, sparkContext: SparkContext, sqlContext: SQLContext, logger: Logger,
          inputDataFrame: DataFrame) = {

    logger.info("Starting DNS suspicious connects analysis.")

    val cleanDataDF = inputDataFrame
      .filter(InputFilter)
      .select(InSchema:_*)
      .na.fill("unknown", Seq(QueryClass))
      .na.fill(-1, Seq(QueryType))
      .na.fill(-1, Seq(QueryResponseCode))

    logger.info("Training the model")

    val model =
      DNSSuspiciousConnectsModel.trainNewModel(sparkContext, sqlContext, logger, config, cleanDataDF, config.topicCount)

    logger.info("Scoring")
    val scoredDF = model.score(sparkContext, sqlContext, cleanDataDF, userDomain)

    val filteredDF = scoredDF.filter(Score + " <= " + config.threshold + " AND " + Score + " > -1")

    val orderedDF = filteredDF.orderBy(Score)

    val mostSuspiciousDF = if(config.maxResults > 0)  orderedDF.limit(config.maxResults) else orderedDF

    val outputDF = mostSuspiciousDF.select(OutSchema:_*).sort(Score)

    logger.info("DNS  suspicious connects analysis completed.")
    logger.info("Saving results to : " + config.hdfsScoredConnect)
    outputDF.map(_.mkString(config.outputDelimiter)).saveAsTextFile(config.hdfsScoredConnect)

    val invalidRecords = inputDataFrame
      .filter(InvalidRecordsFilter)
      .select(InSchema:_*)

    dataValidation.showAndSaveInvalidRecords(invalidRecords, config.hdfsScoredConnect, logger)

    val corruptRecords = scoredDF.filter(Score + " = -1")
    dataValidation.showAndSaveCorruptRecords(corruptRecords, config.hdfsScoredConnect, logger)
  }


  val InStructType = StructType(List(TimestampField, UnixTimestampField, FrameLengthField, ClientIPField,
    QueryNameField, QueryClassField, QueryTypeField, QueryResponseCodeField))

  val InSchema = InStructType.fieldNames.map(col)

  assert(ModelSchema.fields.forall(InStructType.fields.contains(_)))

  val OutSchema = StructType(
    List(TimestampField,
      UnixTimestampField,
      FrameLengthField,
      ClientIPField,
      QueryNameField,
      QueryClassField,
      QueryTypeField,
      QueryResponseCodeField,
      ScoreField)).fieldNames.map(col)

  val InputFilter = s"($Timestamp IS NOT NULL AND $Timestamp <> '' AND $Timestamp <> '-') " +
    s"AND ($UnixTimestamp  IS NOT NULL) " +
    s"AND ($FrameLength IS NOT NULL) " +
    s"AND ($QueryName IS NOT NULL AND $QueryName <> '' AND $QueryName <> '-' AND $QueryName <> '(empty)') " +
    s"AND ($ClientIP IS NOT NULL AND $ClientIP <> '' AND $ClientIP <> '-') " +
    s"AND (($QueryClass IS NOT NULL AND $QueryClass <> '' AND $QueryClass <> '-') " +
    s"OR $QueryType IS NOT NULL " +
    s"OR $QueryResponseCode IS NOT NULL)"

  val InvalidRecordsFilter = s"($Timestamp IS NULL OR $Timestamp = '' OR $Timestamp = '-') " +
    s"OR ($UnixTimestamp  IS NULL) " +
    s"OR ($FrameLength IS NULL) " +
    s"OR ($QueryName IS NULL OR $QueryName = '' OR $QueryName = '-' AND $QueryName = '(empty)') " +
    s"OR ($ClientIP IS NULL OR $ClientIP = '' OR $ClientIP = '-') " +
    s"OR (($QueryClass IS NULL OR $QueryClass = '' OR $QueryClass = '-') " +
    s"AND $QueryType IS NULL " +
    s"AND $QueryResponseCode IS NULL)"
}
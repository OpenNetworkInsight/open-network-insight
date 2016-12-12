package org.apache.spot.netflow

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spot.SuspiciousConnectsArgumentParser.SuspiciousConnectsConfig
import org.apache.spot.netflow.FlowSchema._
import org.apache.spot.netflow.model.FlowSuspiciousConnectsModel
import org.apache.spot.utilities.data.validation.{InvalidDataHandler => dataValidation}

/**
  * The suspicious connections analysis of netflow records develops a probabilistic model the traffic about each
  * IP and flags transactions with an exceptionally low probability in the model as anomalous.
  */

object FlowSuspiciousConnectsAnalysis {

  def run(config: SuspiciousConnectsConfig, sparkContext: SparkContext, sqlContext: SQLContext,
          logger: Logger, inputDataFrame: DataFrame) (implicit outputDelimiter: String) = {

    logger.info("Starting flow suspicious connects analysis.")

    val cleanDataDF = inputDataFrame
      .filter(InputFilter)
      .select(InSchema: _*)

    logger.info("Training the model")

    val model =
      FlowSuspiciousConnectsModel.trainNewModel(sparkContext, sqlContext, logger, config, cleanDataDF, config.topicCount)

    logger.info("Scoring")
    val scoredDF = model.score(sparkContext, sqlContext, cleanDataDF)

    val filteredDF = scoredDF
      .filter(Score + " <= " + config.threshold + " AND " + Score + " > -1 ")

    val orderedDF = filteredDF.orderBy(Score)

    val mostSuspiciousDF = if(config.maxResults > 0 ) orderedDF.limit(config.maxResults) else orderedDF

    val outputDF = mostSuspiciousDF.select(OutSchema: _*)

    logger.info("Netflow  suspicious connects analysis completed.")
    logger.info("Saving results to : " + config.hdfsScoredConnect)
    outputDF.map(_.mkString(config.outputDelimiter)).saveAsTextFile(config.hdfsScoredConnect)

    val invalidRecords = inputDataFrame
      .filter(InvalidRecordsFilter)
      .select(InSchema: _*)
    dataValidation.showAndSaveInvalidRecords(invalidRecords, config.hdfsScoredConnect, logger)

    val corruptRecords = scoredDF.filter(Score + " = -1")
    dataValidation.showAndSaveCorruptRecords(corruptRecords, config.hdfsScoredConnect, logger)

  }


  val InSchema = StructType(List(TimeReceivedField,
    YearField,
    MonthField,
    DayField,
    HourField,
    MinuteField,
    SecondField,
    DurationField,
    SourceIPField,
    DestinationIPField,
    SourcePortField,
    DestinationPortField,
    ProtocolField,
    IpktField,
    IbytField,
    OpktField,
    ObytField)).fieldNames.map(col)

  val OutSchema = StructType(
    List(TimeReceivedField,
      YearField,
      MonthField,
      DayField,
      HourField,
      MinuteField,
      SecondField,
      DurationField,
      SourceIPField,
      DestinationIPField,
      SourcePortField,
      DestinationPortField,
      ProtocolField,
      IpktField,
      IbytField,
      OpktField,
      ObytField,
      ScoreField)).fieldNames.map(col)

  val InputFilter = s"$Hour BETWEEN 0 AND 23 AND $Minute BETWEEN 0 AND 59 AND $Second BETWEEN 0 AND 59 AND " +
    s"$TimeReceived IS NOT NULL AND $SourceIP IS NOT NULL AND $DestinationIP IS NOT NULL AND " +
    s"$SourcePort IS NOT NULL AND $DestinationPort IS NOT NULL AND $Ibyt IS NOT NULL AND $Ipkt IS NOT NULL"

  val InvalidRecordsFilter = s"$Hour BETWEEN 0 AND 23 AND $Minute " +
    s"BETWEEN 0 AND 59 AND $Second BETWEEN 0 AND 59 AND (" +
    s"$TimeReceived IS NULL OR $SourceIP IS NULL OR $DestinationIP IS NULL OR " +
    s"$SourcePort IS NULL OR $DestinationPort IS NULL OR $Ibyt IS NULL OR $Ipkt IS NULL)"
}
package org.apache.spot.utilities.data.validation

import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger

/**
  * Handles invalid and corrupt records.
  * One method for each kind of invalid data, this object prints the total errors and saves the invalid/corrupt records.
  */
object InvalidDataHandler {

  def showAndSaveInvalidRecords(invalidRecords: DataFrame, outputPath: String, logger: Logger) {

    if (invalidRecords.count > 0) {

      val invalidRecordsFile = outputPath + "/invalid_records"
      logger.warn("Saving invalid records to " + invalidRecordsFile)

      invalidRecords.write.mode("overwrite").parquet(invalidRecordsFile)

      logger.warn("Total records discarded due to NULL values in key fields: " + invalidRecords.count +
        " . Please go to " + invalidRecordsFile + " for more details.")
    }
  }

  def showAndSaveCorruptRecords(corruptRecords: DataFrame, outputPath: String, logger: Logger) {
    if(corruptRecords.count > 0){

      val corruptRecordsFile = outputPath + "/corrupt_records"

      logger.warn("Saving corrupt records to " + corruptRecordsFile)

      corruptRecords.write.mode("overwrite").parquet(corruptRecordsFile)

      logger.warn("Total records discarded due to invalid values in key fields: " + corruptRecords.count +
        "Please go to " + corruptRecordsFile + " for more details.")
    }
  }

}

package gx.home

import com.google.common.annotations.VisibleForTesting
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * package
  *
  * @author henry
  * @version 1.0 2019-04-17
  */
package object challenge {
	def loadCSV(spark: SparkSession, path: String, timestampCol: Option[String] = None): DataFrame = {
		println(s"Read CSV file: $path")
		val df = spark.read.format("csv")
				.option("header", "true")
				.option("inferSchema", "true")
				.load(path)

		if (timestampCol.isDefined) df.transform(stringToTimestamp(timestampCol.get))
		else df
	}

	@VisibleForTesting
	def filterWithDate(timestampCol: String, date: String)(df: DataFrame): DataFrame = {
		df.filter(from_unixtime(unix_timestamp(col(timestampCol)), "yyyyMMdd") === date)
	}

	@VisibleForTesting
	def stringToTimestamp(colName: String)(df: DataFrame): DataFrame = {
		df.withColumn("temp",
			unix_timestamp(col(colName), "yyyy-MM-dd HH:mm:ss z").cast(TimestampType))
				.drop(colName)
				.withColumnRenamed("temp", colName)
	}

	/**
	  * divide value with 10**18
	  *
	  * @param valueCol column name to change unit
	  * @param df       DataFrame
	  * @return DataFrame with new column as balance
	  */
	@VisibleForTesting
	def changeUnitFromWei(valueCol: String)(df: DataFrame): DataFrame = {
		df.withColumn("balance", lit(col(valueCol) / math.pow(10, 18)))
	}

}

package gx.home.challenge

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * SparkSessionTestWrapper
  *
  * @author henry
  * @version 1.0 2019-04-18
  */
trait SparkSessionTestWrapper {
	lazy val spark: SparkSession = {
		SparkSession.builder()
				.appName("SparkSession for test")
				.master("local")
				.config("spark.default.parallelism", "1")
				.getOrCreate()
	}

	def setLogLevel(logLevel: String = "WARN"): Unit = {
		spark.sparkContext.setLogLevel(logLevel)
	}

	def printDF(df: DataFrame, truncate: Boolean = false): Unit = {
		df.printSchema()
		df.show(truncate)
	}
}

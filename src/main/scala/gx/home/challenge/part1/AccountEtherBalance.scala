package gx.home.challenge.part1

import gx.home.challenge._
import org.apache.spark.sql.{SaveMode, SparkSession}

object AccountEtherBalance extends App {

	val param = AccountEtherBalanceParam().getParams(this, args)

	val spark = SparkSession.builder().getOrCreate()

	val transDF = loadCSV(spark, param.input, Some("block_timestamp"))

	val resDF = getBalanceByAccount(transDF, param.date)

	resDF.coalesce(1).write
			.format("csv")
			.mode(SaveMode.Overwrite)
			.save(s"${param.output}/${param.date}")
}
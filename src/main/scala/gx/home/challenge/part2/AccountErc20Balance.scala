package gx.home.challenge.part2

import gx.home.challenge.loadCSV
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
  * AccountErc20Balance
  *
  * @author henry
  * @version 1.0 2019-04-18
  */
object AccountErc20Balance extends App {
	val param = AccountErc20BalanceParam().getParams(this, args)
	val spark = SparkSession.builder().getOrCreate()

	val contractsDF = loadCSV(spark, param.contractsTable, Some("block_timestamp"))
	val tokenTransfersDF = loadCSV(spark, param.tokenTransfersTable, Some("block_timestamp"))
	val tokensDF = loadCSV(spark, param.tokensTable)

	val resDF = getAccountErc20Balance(contractsDF, tokenTransfersDF, tokensDF, param.date)

	// to debug
	resDF.printSchema()
	resDF.show()

	resDF.coalesce(1).write
			.format("csv")
			.mode(SaveMode.Overwrite)
			.save(s"${param.output}/${param.date}")
}

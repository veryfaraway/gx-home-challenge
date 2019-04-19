package gx.home.challenge.part2

import gx.home.challenge.{SparkSessionTestWrapper, _}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite


/**
  * Part2Test
  *
  * @author henry
  * @version 1.0 2019-04-19
  */
class Part2Test extends FunSuite with SparkSessionTestWrapper {

	val contractsDF: DataFrame = loadCSV(spark, "data/input/contracts.csv", Some("block_timestamp"))
	val tokenTransfersDF: DataFrame = loadCSV(spark, "data/input/token_transfers.csv", Some("block_timestamp"))

	test("test erc20") {
		val resDF = contractsDF.transform(getErc20Contracts)
		printDF(resDF)
	}

	test("test daily token transfers") {
		val resDF = tokenTransfersDF.transform(getDailyTransactions("block_timestamp", "20160208"))
		printDF(resDF)
	}

	test("join contracts with transfers") {
		val erc20DF = contractsDF.transform(getErc20Contracts)
		val dailyTokenTransfersDF = tokenTransfersDF.transform(getDailyTransactions("block_timestamp", "20160208"))

		val resDF = erc20DF.transform(joinTransfers(dailyTokenTransfersDF))

		printDF(resDF)
	}



}

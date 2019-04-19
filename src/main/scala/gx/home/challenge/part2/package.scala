package gx.home.challenge

import com.google.common.annotations.VisibleForTesting
import gx.home.challenge.part1._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * package
  *
  * @author henry
  * @version 1.0 2019-04-18
  */
package object part2 {

	def getAccountErc20Balance(contractsDF: DataFrame, tokenTransfersDF: DataFrame, tokensDF: DataFrame,
							   date: String): DataFrame = {

		val dailyContractsDF = contractsDF.transform(getErc20Contracts)

		val dailyTokenTransfersDF = tokenTransfersDF.transform(getDailyTransactions("block_timestamp", date))

		dailyContractsDF.transform(joinTransfers(dailyTokenTransfersDF))
				.transform(changeUnitFromWei("value"))
				.transform(checkBalance)
	}

	@VisibleForTesting
	def getErc20Contracts(df: DataFrame): DataFrame = {
		df.select(
			col("address"),
			col("is_erc20")
		).filter(col("is_erc20") === true)
	}

	@VisibleForTesting
	def getDailyTransactions(timestampCol: String, date: String)(df: DataFrame): DataFrame = {
		df.select(
			col("token_address"),
			col("from_address"),
			col("to_address"),
			col("block_timestamp"),
			col("value")
		).transform(filterWithDate(timestampCol, date))
	}

	@VisibleForTesting
	def joinTransfers(transfersDF: DataFrame)(contractDF: DataFrame): DataFrame = {
		contractDF.join(transfersDF, contractDF("address") === transfersDF("token_address"))
				.select(
					transfersDF("token_address"),
					contractDF("is_erc20"),
					transfersDF("from_address"),
					transfersDF("to_address"),
					transfersDF("value"),
					transfersDF("block_timestamp"))
	}

}

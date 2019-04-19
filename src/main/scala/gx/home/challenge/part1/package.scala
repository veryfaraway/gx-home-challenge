package gx.home.challenge

import com.google.common.annotations.VisibleForTesting
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

/**
  * package
  *
  * @author henry
  * @version 1.0 2019-04-17
  */
package object part1 {

	case class AccountBalance(account: String, balance: Double)

	def getBalanceByAccount(transactionDF: DataFrame, date: String): DataFrame = {
		transactionDF.transform(getDailyTransactions("block_timestamp", date))
				.filter(col("from_address").isNotNull)
				.filter(col("to_address").isNotNull)
				.filter(col("value").isNotNull)
				.transform(changeUnitFromWei("value"))
				.transform(checkBalance)
	}

	@VisibleForTesting
	def getDailyTransactions(timestampCol: String, date: String)(df: DataFrame): DataFrame = {
		df.select(
			col("from_address"),
			col("to_address"),
			col("block_timestamp"),
			col("value")
		).transform(filterWithDate(timestampCol, date))
	}

	def checkBalance(df: DataFrame): DataFrame = {
		df.select(
			cal_balance(
				col("from_address"), col("to_address"), col("balance")
			).as("abList")
		).select(
			explode(col("abList")).as("ab")
		).select(
			col("ab.account").as("account"),
			col("ab.balance").as("balance")
		).groupBy("account")
				.agg(sum(col("balance")).as("balance"))
	}

	def cal_balance: UserDefinedFunction = udf { (from: String, to: String, balance: Double) =>
		Seq(AccountBalance(from, -balance), AccountBalance(to, balance))
	}
}

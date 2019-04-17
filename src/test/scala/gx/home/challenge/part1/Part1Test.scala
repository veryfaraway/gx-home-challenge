package gx.home.challenge.part1

import gx.home.challenge.{SparkSessionTestWrapper, _}
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

/**
  * Part1Test
  *
  * @author henry
  * @version 1.0 2019-04-18
  */
class Part1Test extends FunSuite with SparkSessionTestWrapper {

	import spark.implicits._

	val df: DataFrame = Seq(
		("1", "2", "2015-11-20 06:04:52 UTC", BigInt("98641439999990000")),
		("3", "4", "2015-11-21 05:09:13 UTC", BigInt("129347739989999000000"))
	).toDF("from_address", "to_address", "block_timestamp", "value")

	test("filter timestamp") {

		val resDF = df.transform(stringToTimestamp("block_timestamp"))
				.transform(filterWithDate("20151121"))

		printDF(resDF)
		assert(resDF.count() == 1)
	}

	test("change unit") {
		val resDF = df.transform(changeUnitFromWei("value"))
		printDF(resDF)

		val balanceList = resDF.select("balance")
        		.collectAsList()

		assert(balanceList.get(0).getAs[Double](0) == 0.09864143999999)
		assert(balanceList.get(1).getAs[Double](0) == 129.34773998999898)
	}

	test("")

}

package gx.home.challenge

import org.scalatest.FunSuite

/**
  * HomeChallengeTest
  *
  * @author henry
  * @version 1.0 2019-04-18
  */
class HomeChallengeTest extends FunSuite with SparkSessionTestWrapper {

	import spark.implicits._

	test("covert to timestamp") {
		val df = Seq("2015-11-20 06:04:52 UTC")
				.toDF("block_timestamp")

		val timestampDF = df.transform(stringToTimestamp("block_timestamp"))

		printDF(timestampDF)
	}

}

package gx.home.challenge.part1

import gx.home.challenge.common.JobParameter
import scopt.OptionParser

/**
  * AccountEtherBalanceParam
  *
  * @author henry
  * @version 1.0 2019-04-17
  */
case class AccountEtherBalanceParam(input: String = null,
									date: String = null,
									output: String = null)
		extends JobParameter[AccountEtherBalanceParam] {

	override protected def parseParams(parser: OptionParser[AccountEtherBalanceParam]): AccountEtherBalanceParam = {
		parser.opt[String]('i', "input").required()
				.valueName("<path>")
				.action((x, c) => c.copy(input = x))
				.text("input path")

		parser.opt[String]('o', "output").required()
				.valueName("<path>")
				.action((x, c) => c.copy(output = x))
				.text("output path")

		parser.opt[String]("date").required()
				.valueName("<yyyyMMdd>")
				.action((x, c) => c.copy(date = x))
				.text("transaction date")

		this
	}
}

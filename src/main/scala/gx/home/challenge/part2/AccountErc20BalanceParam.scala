package gx.home.challenge.part2

import gx.home.challenge.common.JobParameter
import scopt.OptionParser

/**
  * AccountErc20BalanceParam
  *
  * @author henry
  * @version 1.0 2019-04-18
  */
case class AccountErc20BalanceParam(contractsTable: String = null,
									tokenTransfersTable: String = null,
									tokensTable: String = null,
									date: String = null,
									output: String = null)
		extends JobParameter[AccountErc20BalanceParam] {

	override protected def parseParams(parser: OptionParser[AccountErc20BalanceParam]): AccountErc20BalanceParam = {
		parser.opt[String]("inputContracts").required()
				.valueName("<path>")
				.action((x, c) => c.copy(contractsTable = x))
				.text("input path of contracts")

		parser.opt[String]("inputTokenTransfers").required()
				.valueName("<path>")
				.action((x, c) => c.copy(tokenTransfersTable = x))
				.text("input path of tokenTransfers")

		parser.opt[String]("inputTokens").required()
				.valueName("<path>")
				.action((x, c) => c.copy(tokensTable = x))
				.text("input path of tokens")

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

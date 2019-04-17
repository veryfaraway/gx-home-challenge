package gx.home.challenge.common

import scopt.OptionParser

/**
  * JobParameter
  *
  * @author henry
  * @version 1.0 2019-04-17
  */
trait JobParameter[C] {
	// default values of project, override this values to suit your project
	var projectName: String = "scopt"
	var projectVer: String = "3.x"

	protected def parseParams(parser: OptionParser[C]): C

	def getParams[T](job: T, args: Seq[String]): C = {
		val parser = getParser(job)
		val init = parseParams(parser)

		parser.parse(args, init) match {
			case Some(params) =>
				println("OptionParser: Parsing args is successful!!")
				println(params)
				params
			case None =>
				// arguments are bad, error message will have been displayed
				sys.exit(1)
		}
	}

	protected def getParser[T](job: T): OptionParser[C] = {
		val className = job.getClass.getName.split("\\$").last
		val jobName = job.getClass.getSimpleName.split("\\$").last

		new OptionParser[C](className) {
			head(projectName, s"($projectVer)", jobName)
		}
	}
}

package io.mola.spark.checksum

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

case class Config(
    algorithm: String = null,
    check: Boolean = false,
    checksums: String = null,
    base: Option[String] = None,
    paths: Seq[String] = Nil
)

object App extends Logging {

  val AppName = "checksum-spark"

  def main(args: Array[String]): Unit = {
    val config = parseConfig(args).getOrElse(sys.exit(1))

    val spark = SparkSession.builder
      .appName(AppName)
      .getOrCreate()
    implicit val sc = spark.sparkContext

    run(config)
  }

  def parseConfig(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config](AppName) {
      opt[Boolean]('c', "check")
        .text("read check sums from the FILEs and check them")
        .action((v, c) => c.copy(check = v))
        .validate({
          case true  => Right()
          case false => Left("only check mode is supported")
        })

      opt[String]('a', "algorithm")
        .text("checksum algorithm")
        .withFallback(() => "md5")
        .action((v, c) => c.copy(algorithm = v))

      opt[String]('b', "base")
        .text("base directory")
        .optional()
        .action((v, c) => c.copy(base = Some(v)))

      arg[String]("<checksums path>")
        .text("path of checksum file")
        .minOccurs(1)
        .maxOccurs(1)
        .action((v, c) => c.copy(checksums = v))

      arg[String]("<path>")
        .text("paths of files to checksum (or in check mode, path to checksum files)")
        .required()
        .minOccurs(1)
        .maxOccurs(1)
        .action((v, c) => c.copy(paths = c.paths :+ v))

      help("help").text("prints this usage text")
      override def showUsageOnError = true
    }

    parser.parse(args, Config())
  }

  def run(config: Config)(implicit sc: SparkContext): Unit = {
    sc.hadoopConfiguration
      .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val resolvedPaths = config.base match {
      case None => config.paths
      case Some(base) =>
        config.paths.map(p => new Path(new Path(base), p).toString)
    }

    val notMatch = Checksum
      .check(
        config.checksums,
        config.base,
        resolvedPaths,
        config.algorithm
      )
      .toLocalIterator

    var failures = 0
    for (nm <- notMatch) {
      if (nm.matched) {
        println(s"${nm.path}: OK")
      } else {
        println(s"${nm.path}: FAILED")
        failures += 1
      }
    }

    if (failures > 0) {
      Console.withOut(Console.err) {
        println(
          s"checksum-spark: WARNING: $failures computed checksums did NOT match")
      }
    }
  }

}

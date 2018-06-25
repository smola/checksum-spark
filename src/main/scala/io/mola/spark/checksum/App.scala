package io.mola.spark.checksum

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

case class Config(
    algorithm: String = null,
    ignoreMissing: Boolean = false,
    quiet: Boolean = false,
    status: Boolean = false,
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

    val result = run(config)
    if (!result) {
      sys.exit(1)
    }
  }

  def parseConfig(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config](AppName) {
      opt[Unit]('c', "check")
        .text("read check sums from the FILEs and check them")
        .action((v, c) => c.copy(check = true))

      opt[Unit]("ignore-missing")
        .text("don't fail or report status for missing files")
        .optional()
        .action((_, c) => c.copy(ignoreMissing = true))

      opt[Unit]("quiet")
        .text("don't print OK for each successfully verified file")
        .optional()
        .action((_, c) => c.copy(quiet = true))

      opt[Unit]("status")
        .text("don't output anything, status code shows success")
        .optional()
        .action((_, c) => c.copy(status = true))

      opt[String]("algorithm")
        .text("checksum algorithm")
        .withFallback(() => "md5")
        .action((v, c) => c.copy(algorithm = v))

      opt[String]("base")
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

  def run(config: Config)(implicit sc: SparkContext): Boolean = {
    sc.hadoopConfiguration
      .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val resolvedPaths = config.base match {
      case None => config.paths
      case Some(base) =>
        config.paths.map(p => new Path(new Path(base), p).toString)
    }

    val newConfig = config.copy(paths = resolvedPaths)
    if (config.check) {
      check(newConfig)
    } else {
      compute(newConfig)
    }
  }

  def compute(config: Config)(implicit sc: SparkContext): Boolean = {
    Checksum
      .compute(
        config.base,
        config.paths,
        config.algorithm
      )
      .map({ case (path, hash) => s"$hash  $path" })
      .saveAsTextFile(config.checksums)
    true
  }

  def check(config: Config)(implicit sc: SparkContext): Boolean = {

    val results = Checksum
      .check(
        config.checksums,
        config.base,
        config.paths,
        config.algorithm
      )
      .toLocalIterator

    var unmatched = 0
    var missing = 0
    results.foreach({
      case Match(path, _) =>
        if (!config.quiet && !config.status) {
          println(s"$path: OK")
        }
      case NotMatch(path, _, _) =>
        if (!config.status) {
          println(s"$path: FAILED")
        }
        unmatched += 1
      case MissingMatch(path, _) =>
        if (!config.ignoreMissing && !config.status) {
          println(s"$path: FAILED open or read")
        }
        missing += 1
    })

    Console.withOut(Console.err) {
      if (unmatched > 0) {
        println(
          s"checksum-spark: WARNING: $unmatched computed checksums did NOT match")
      }

      if (!config.ignoreMissing && missing > 0) {
        println(
          s"checksum-spark: WARNING: $missing listed file could not be read")
      }
    }

    unmatched == 0 && (config.ignoreMissing || missing == 0)
  }

}

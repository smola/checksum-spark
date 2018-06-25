package io.mola.spark.checksum

import java.security.MessageDigest

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Checksum {

  def checksumFile(path: String)(
      implicit sc: SparkContext): RDD[(String, String)] = {
    sc.textFile(path, minPartitions = 1000)
      .map(line => {
        val fields = line.split("\\s+", 2)
        fields(1) -> fields(0)
      })
  }

  def relativize(base: Option[String], path: String): String = {
    base match {
      case None => path
      case Some(b) =>
        val baseUri = new Path(b).toUri
        val pathUri = new Path(path).toUri
        baseUri.relativize(pathUri).toString
    }
  }

  def compute(base: Option[String], paths: Seq[String], algorithm: String)(
      implicit sc: SparkContext): RDD[(String, String)] = {
    paths
      .map(sc.binaryFiles(_))
      .reduce(_.union(_))
      .map({
        case (path, stream) =>
          val md = MessageDigest.getInstance(algorithm)
          val buf = new Array[Byte](512 * 1024)
          val in = stream.open()
          Stream
            .continually(in.read(buf))
            .takeWhile(_ > 0)
            .foreach({ read =>
              md.update(buf.slice(0, read))
            })
          val digest = md.digest().map("%02x".format(_)).mkString

          (relativize(base, path), digest)
      })

  }

  def check(checksumPath: String,
            base: Option[String],
            paths: Seq[String],
            algorithm: String)(implicit sc: SparkContext): RDD[BaseMatch] = {
    checksumFile(checksumPath)
      .leftOuterJoin(compute(base, paths, algorithm))
      .map({
        case (path, (a, None))              => MissingMatch(path, a)
        case (path, (a, Some(b))) if a == b => Match(path, a)
        case (path, (a, Some(b)))           => NotMatch(path, a, b)
      })
  }

}

sealed trait BaseMatch {
  def path: String
  def expected: String
  def matched: Boolean
}

case class Match(override val path: String, override val expected: String)
    extends BaseMatch {

  override def matched: Boolean = true

}

case class MissingMatch(override val path: String,
                        override val expected: String)
    extends BaseMatch {

  override def matched: Boolean = false
}

case class NotMatch(override val path: String,
                    override val expected: String,
                    actual: String)
    extends BaseMatch {

  override def matched: Boolean = false
}

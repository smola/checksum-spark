package io.mola.spark.checksum

import java.io.File

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

import scala.io.Source

class ChecksumSuite extends FunSuite with SharedSparkContext {

  lazy val cwd = new File(".").getAbsolutePath
  lazy val fixturesPath = s"$cwd/src/test/resources"

  test("relativize") {
    assert(Checksum.relativize(None, "foo") == "foo")
    assert(Checksum.relativize(None, "*") == "*")
    assert(Checksum.relativize(Some("foo"), "foo/bar") == "bar")
    assert(Checksum.relativize(Some("foo"), "foo/*") == "*")
    assert(Checksum.relativize(Some("file:/foo"), "file:/foo/bar") == "bar")
    assert(Checksum.relativize(Some("file:/foo"), "file:/foo/*") == "*")
  }

  test("compute checksums") {
    implicit val sc = this.sc
    val result = Checksum
      .compute(
        Some(s"file:$fixturesPath/test1"),
        Seq(s"file:$fixturesPath/test1/*"),
        "md5"
      )
      .collect()

    assert(result.length == 100)

    val expected = Source
      .fromFile("src/test/resources/test1.md5")
      .getLines()
      .toSeq
      .sorted
      .mkString("\n")
    val actual = result.map({ case (a, b) => s"$b  $a" }).sorted.mkString("\n")
    assert(actual == expected)
  }

  test("check all good") {
    implicit val sc = this.sc
    val result = Checksum
      .check(
        "src/test/resources/test1.md5",
        Some(s"file:$fixturesPath/test1"),
        Seq(s"file:$fixturesPath/test1/*"),
        "md5"
      )
      .collect()

    assert(result.length == 100)
    assert(result.count(_.matched) == 100)
  }

  test("check some bad") {
    implicit val sc = this.sc
    val result = Checksum
      .check(
        "src/test/resources/test1_5bad.md5",
        Some(s"file:$fixturesPath/test1"),
        Seq(s"file:$fixturesPath/test1/*"),
        "md5"
      )
      .collect()

    assert(result.length == 100)
    assert(result.count(_.matched) == 95)
  }

  test("check one missing") {
    implicit val sc = this.sc
    val result = Checksum
      .check(
        "src/test/resources/test1_one_extra.md5",
        Some(s"file:$fixturesPath/test1"),
        Seq(s"file:$fixturesPath/test1/*"),
        "md5"
      )
      .collect()

    assert(result.length == 101)
    assert(result.count(_.matched) == 100)
    assert(result.count(_.isInstanceOf[MissingMatch]) == 1)
  }

}

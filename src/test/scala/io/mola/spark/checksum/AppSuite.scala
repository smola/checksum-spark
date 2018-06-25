package io.mola.spark.checksum

import java.io.{ByteArrayOutputStream, File}

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

class AppSuite extends FunSuite with SharedSparkContext {

  lazy val cwd = new File(".").getAbsolutePath
  lazy val fixturesPath = s"$cwd/src/test/resources"

  test("parse config with errors") {
    val bufErr = new ByteArrayOutputStream()
    Console.withErr(bufErr) {
      assert(App.parseConfig(Array()).isEmpty)
    }
    val expected =
      """Error: Missing argument <checksums path>
                     |Error: Missing argument <path>
                     |Usage: checksum-spark [options] <checksums path> <path>
                     |
                     |  -c, --check          read check sums from the FILEs and check them
                     |  --ignore-missing     don't fail or report status for missing files
                     |  --quiet              don't print OK for each successfully verified file
                     |  --status             don't output anything, status code shows success
                     |  --algorithm <value>  checksum algorithm
                     |  --base <value>       base directory
                     |  <checksums path>     path of checksum file
                     |  <path>               paths of files to checksum (or in check mode, path to checksum files)
                     |  --help               prints this usage text
                     |""".stripMargin
    assert(new String(bufErr.toByteArray) == expected)
  }

  test("parse config") {
    assert(
      App
        .parseConfig(
          Array("src/test/resources/test1.md5", "src/test/resources/test1/*"))
        .get == Config(
        algorithm = "md5",
        checksums = "src/test/resources/test1.md5",
        paths = Seq("src/test/resources/test1/*")
      ))
  }

  test("run") {
    implicit val sc = this.sc
    val bufOut = new ByteArrayOutputStream()
    val config = Config(
      algorithm = "md5",
      check = true,
      checksums = s"file:$fixturesPath/test1.md5",
      base = Some(s"file:$fixturesPath/test1"),
      paths = Seq(s"file:$fixturesPath/test1/*")
    )
    Console.withOut(bufOut) {
      App.run(config)
    }
    val out = new String(bufOut.toByteArray)
    assert(!out.contains("FAILED"))
    assert(out.contains("OK"))
  }

}

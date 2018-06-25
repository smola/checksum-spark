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
                     |  -c, --check <value>      read check sums from the FILEs and check them
                     |  -a, --algorithm <value>  checksum algorithm
                     |  -b, --base <value>       base directory
                     |  <checksums path>         path of checksum file
                     |  <path>                   paths of files to checksum (or in check mode, path to checksum files)
                     |  --help                   prints this usage text
                     |""".stripMargin
    assert(new String(bufErr.toByteArray) == expected)
  }

  test("parse config") {
    assert(
      App
        .parseConfig(
          Array("src/test/resources/test1.md5", "src/test/resources/test1"))
        .get == Config(
        algorithm = "md5",
        checksums = "src/test/resources/test1.md5",
        paths = Seq("src/test/resources/test1")
      ))
  }

  test("run") {
    implicit val sc = this.sc
    val config = Config(
      algorithm = "md5",
      checksums = s"file:$fixturesPath/test1.md5",
      paths = Seq(s"file:$fixturesPath/test1")
    )
    App.run(config)
  }

}

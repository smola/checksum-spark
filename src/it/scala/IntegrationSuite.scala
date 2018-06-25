import java.io.{ByteArrayOutputStream, File}

import com.holdenkarau.spark.testing.SharedSparkContext
import io.mola.spark.checksum.App
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.FeatureSpec

class IntegrationSuite extends FeatureSpec with SharedSparkContext {

  lazy val hdfsBase = "hdfs://localhost:9000"

  override def beforeAll(): Unit = {
    super.beforeAll()
    this.sc.hadoopConfiguration.set("fs.defaultFS", hdfsBase)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(s"$hdfsBase/test"), true)
    fs.mkdirs(new Path("test/test1"))
    new File("src/test/resources")
      .list()
      .foreach({ path =>
        fs.copyFromLocalFile(new Path(s"src/test/resources/$path"),
                             new Path(s"/test/$path"))
      })
  }

  feature("checksum on HDFS") {
    scenario("all matches") {
      val buf = new ByteArrayOutputStream()
      Console.withOut(buf) {
        App.main(
          Array(
            "--check",
            "--base",
            s"$hdfsBase/test/test1",
            s"$hdfsBase/test/test1.md5",
            s"$hdfsBase/test/test1/*"
          ))
      }
      val output = new String(buf.toByteArray)
      assert(!output.contains("FAILED"))
      assert(output.contains("OK"))
    }
  }

}

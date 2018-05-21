package poc
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.log4j.{Level, Logger}


object SparkContextPoc {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)


    val conf = new SparkConf(true).setMaster("local")
      .setAppName("POC").set("spark.driver.memory","1g")

    val context = new SparkContext(conf)

    println(conf.get("spark.driver.memory"))
  }

}

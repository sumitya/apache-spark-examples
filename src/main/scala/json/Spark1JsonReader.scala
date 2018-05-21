package json

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Spark1JsonReader {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)

    // spark 1 code

    val sconf = new SparkConf().setAppName("Json").setMaster("local")
    val sc = new SparkContext(sconf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)


    val file = sqlContext.read.json(sc.wholeTextFiles("src/resources/sample.json").values)

    file.take(5).foreach(println)

  }

}

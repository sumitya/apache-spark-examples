package json

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Spark2JsonReader {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()


    // reading a json
    val df = spark.read.option("multiline",true).json("src/resources/sample.json")

    df.printSchema()

    df.createOrReplaceGlobalTempView("JSON_TABLE")

    df.select("select type from JSON_TABLE").show()





  }

}

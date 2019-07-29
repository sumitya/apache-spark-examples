package poc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import util.GetAllProperties

object RDDOperations {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.DEBUG)

    Logger.getLogger("com").setLevel(Level.DEBUG)

    var inputFile = GetAllProperties.readPropertyFile get "INPUT_FILE" getOrElse("#")

    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .master("local")
      .getOrCreate()

val arrayRDD = spark.sparkContext.parallelize(Array(1,2,3,4,5,6,7,8,9,10))

    val flatMapRDD = arrayRDD.flatMap(something => something.toString.split(""))

    flatMapRDD.take(10)


  }

}

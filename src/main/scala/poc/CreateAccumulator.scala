package poc

import org.apache.spark.sql.SparkSession
import util.GetAllProperties

object CreateAccumulator extends App{

  val spark = SparkSession
    .builder
    .appName("CreateAccumulator")
    .master("local[1]")
    .getOrCreate()

  val userName = System.getProperty("user.name")

  var userinfofile = GetAllProperties.readPropertyFile get "USERINFOFILE" getOrElse ("#") replace("<USER_NAME>", userName)

  val file = spark.sparkContext.textFile(userinfofile)

  val scalaWords = spark.sparkContext.accumulator(0)

  val counts = file.map{
  row =>

      if(row.contains("Scala")){
        scalaWords+=1
      }
      println(scalaWords.value)
  }

  counts.take(10)

  println("Scala Count is: "+scalaWords.value)

  Thread.sleep(10000000)

}

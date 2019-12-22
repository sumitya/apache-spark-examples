package definitiveguide.aggregations_7

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import util.GetAllProperties

object DataFrameAggregates extends App{
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession
    .builder
    .appName("workingWithDifferentData")
    .master("local[1]")
    .getOrCreate()

  // get input file location var
  val userName = System.getProperty("user.name")
  var csvFile = GetAllProperties.readPropertyFile get "CSV_FILE" getOrElse ("#") replace("<USER_NAME>", userName)

  //Assign schema to a DataFrame

  val schema = StructType(Array(
    StructField("ID", IntegerType, false),
    StructField("NAME", StringType, true),
    StructField("AMOUNT", DoubleType, false, Metadata.fromJson("{\"hello\":\"world\"}"))
  )
  )

  val df = spark.read.format("csv").schema(schema).load(csvFile)

  df.cache()


  println(df.count())  //  -> it is same as count(*), Spark will count the null values too.

  import org.apache.spark.sql.functions.count

  df.select(count("ID")).show() // -> when counting an individual column, Spark will not count the null values.



}

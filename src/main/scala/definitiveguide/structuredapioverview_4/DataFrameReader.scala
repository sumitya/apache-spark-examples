package definitiveguide.structuredapioverview_4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import util.GetAllProperties

object DataFrameReader extends App{

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession
    .builder
    .appName("TestSchemaInfer")
    .master("local[1]")
    .getOrCreate()

  spark.conf.set("spark.sql.shuffle.partitions", "5")

  // get input file location var
  val userName = System.getProperty("user.name")

  var csvFile = GetAllProperties.readPropertyFile get "CSV_FILE" getOrElse ("#") replace("<USER_NAME>", userName)

  var parquetFile = GetAllProperties.readPropertyFile get "PARQUET_FILE" getOrElse ("#") replace("<USER_NAME>", userName)

  val csvData = spark.read
    .option("header","true")
    .csv(csvFile)

  csvData.sort("id")

  csvData.show()

  println(csvData.rdd.getNumPartitions)

  case class User(ID:String,NAME:String,AMOUNT:String)

  val columnNames = classOf[User].getDeclaredFields.map(x => x.getName)

  //read csv with and assign strong type

  val df = spark.read
    //.option("header",false)
    //.option("inferSchema",true)
    .csv(csvFile)
    .toDF(columnNames:_*)

  import spark.implicits._

  df.printSchema()

  val users = df.as[User]

  users.write.mode(SaveMode.Overwrite).parquet(parquetFile)

  users.printSchema()

  val parquetData = spark.read.parquet(parquetFile)

  import spark.implicits._

  val parquetFileWithSchema = parquetData.as[User]

  parquetData.printSchema()

  parquetData.show()

  parquetFileWithSchema.filter(_.NAME == "abc").show


  Thread.sleep(2000000)

  spark.stop()

}

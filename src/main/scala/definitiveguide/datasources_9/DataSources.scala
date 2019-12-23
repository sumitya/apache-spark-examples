package definitiveguide.datasources_9

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import util.GetAllProperties

object DataSources extends App{

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession
    .builder
    .appName("DifferentDataSource")
    .master("local[1]")
    .getOrCreate()

  // get input file location var
  val userName = System.getProperty("user.name")
  var csvFile = GetAllProperties.readPropertyFile get "CSV_FILE" getOrElse ("#") replace("<USER_NAME>", userName)
  var outputPath = GetAllProperties.readPropertyFile get "OUTPUT_PATH" getOrElse ("#") replace("<USER_NAME>", userName)

  val schema = StructType(Array(
    StructField("ID", IntegerType, false),
    StructField("NAME", StringType, true),
    StructField("AMOUNT", DoubleType, false, Metadata.fromJson("{\"hello\":\"world\"}"))
    )
  )

  // Read API Structure Structure:: DataFrameReader.format(...).option("key", "value").schema(...).load()

  // format,option,schema all are optional, minimum, you must supply the DataFrameReader a path to from which to read.
  // Spark’s read modes ->
  // 1. permissive - all fields to null when it encounters a corrupted record, _corrupt_record string is mentioned for corrupt.
  // 2. dropMalformed - Drops the row that contains malformed records
  // 3. failFast - Fails immediately upon encountering malformed records
  // default is permissive

  val df = spark.read.format("csv")
    //.option("mode","FAILFAST")
    .option("inferSchema", "true")
    .option("path", csvFile)
    .schema(schema)
    .load()


  df.show()

  // Write API Structure :: DataFrameWriter.format(...).option(...).partitionBy(...).bucketBy(...).sortBy(...).save()

  // Spark’s save modes
  // append ->
  // overwrite
  // errorIfExists -> Throws an error and fails the write if data or files already exist at the specified location
  // ignore -> If data or files exist at the location, do nothing with the current DataFrame
  // The default is errorIfExists.

  df.write.format("csv").option("mode", "OVERWRITE").option("path",outputPath).save()

  // CSV

  // https://github.com/databricks/spark-csv -> this has all the option to read/write csv.




}

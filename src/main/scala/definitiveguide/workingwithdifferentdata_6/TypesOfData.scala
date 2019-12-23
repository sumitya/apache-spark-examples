package definitiveguide.workingwithdifferentdata_6

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import util.GetAllProperties

object TypesOfData extends App {

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

  import org.apache.spark.sql.functions.col

  df.where(col("ID").equalTo("123")).select("ID", "NAME").show()

  // in Spark with Col objects
  // == returns a boolean
  // === returns a column (which contains the result of the comparisons of the elements of two columns)

  df.where(col("ID") === "123").select("ID", "NAME").show()
  // df.where(col("ID")=="123").select("ID","NAME").show() // --> == returns a boolean ,it doesn't compile.

  df.where(col("ID") =!= "123").select("ID", "NAME").show()

  // probably the cleanes way is to specify the predicate as an expression in a string.
  df.where("ID = 123").select("ID", "NAME").show()
  df.where("ID <> 123").select("ID", "NAME").show()

  // Booleans

  import org.apache.spark.sql.functions.{expr, not, col}

  val isValidNameDF = df.withColumn("IsValidName", not(col("NAME").equalTo("abc")))

  isValidNameDF.show()
  isValidNameDF.where("IsValidName").show() // -> its going to give back records with true value of IsValidName column.
  isValidNameDF.where(col("IsValidName").eqNullSafe("abc")).show()

  /*if you’re working with null data when creating Boolean expressions.
  If there is a null in your data, you’ll need to treat things a bit differently.
  Here’s how you can ensure that you perform a null-safe equivalence test:
  */

  // Numbers

  import org.apache.spark.sql.functions.{round, bround, lit}

  // round down -> bround

  df.select(round(col("AMOUNT"), 1)).alias("AMOUNT_ROUNDED").show()
  df.select(bround(lit("2.5"))).show()

  // compute summary statistics for a column or set of columns

  df.describe().show()

  // String

  import org.apache.spark.sql.functions.{initcap, trim}

  df.select(initcap(col("NAME"))).show()
  df.select(trim(lit("HELLO  ")))

  // varargs

  def echo(args: String*) =
    for (arg <- args) println(arg)

  val arr = Array("What's", "up", "doc?")
  echo(arr: _*)

  // date and timestamp

  val dateDF = spark.range(10)

  import org.apache.spark.sql.functions.{current_date, current_timestamp}


  val dateTimeDF = dateDF.withColumn("TODAY_DATE", current_date()).withColumn("NOW", current_timestamp())

  import org.apache.spark.sql.functions.{date_add, date_sub}

  dateTimeDF.select(date_sub(col("TODAY_DATE"), 5), date_add(col("TODAY_DATE"), 5)).show(1)

  import org.apache.spark.sql.functions.{to_date, to_timestamp}

  // Spark will not throw an error if it cannot parse the date; rather, it will just return null. This can be a bit tricky in larger pipelines
  dateTimeDF.select(to_date(lit("2016-20-12")), to_date(lit("2017-12-11"))).show()

  //There are two functions to fix this: to_date and to_timestamp

  val dateFormat = "yyyy-dd-MM"

  dateDF.select(to_date(lit("2016-20-12"), dateFormat)).show()
  dateDF.select(to_timestamp(lit("2016-20-12"), dateFormat)).show()


  // null

  import org.apache.spark.sql.functions.coalesce

  df.select(coalesce(col("NAME"), col("ID"))).show() // coalesce -> Returns the first column that is not null, or null if all inputs are null.
  df.na.drop().show() //  -> this will any null row

  // Struct

  import org.apache.spark.sql.functions.struct

  df.select(struct("ID", "AMOUNT").alias("complex_type")).show()

  // Array
  import org.apache.spark.sql.functions.split

  df.select(split(lit("This is a column"), " ").alias("array_col")).selectExpr("array_col[1]").show()

  // explode

  // This is a column -> split -> ["This", "is", "a", "column"] , "other col" -> explode  (This, "other col")("is","other col")

  import org.apache.spark.sql.functions.explode

  df.select(explode(split(lit("This is a column"), " ").alias("array_col")), col("ID")).show()

  // Map
  import org.apache.spark.sql.functions.map

  val header = df.first()
  val withoutheader = df.filter(row => row!=header)
  withoutheader.select(map(col("ID"), col("NAME")).alias("complex_map")).show(2)

  // converting struct into json

  import org.apache.spark.sql.functions.to_json

  df.selectExpr("(ID, NAME) as myStruct")
    .select(to_json(col("myStruct"))).show()


  // UDF
  def power3(number:Double):Double = number * number * number
  power3(2.0)

  import org.apache.spark.sql.functions.udf
  val power3udf = udf(power3(_:Double):Double)

  // in Scala
 df.select(power3udf(col("ID"))).show()

}

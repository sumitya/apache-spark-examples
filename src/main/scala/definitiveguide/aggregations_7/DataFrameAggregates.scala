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

  // grouping with aggregates

  import org.apache.spark.sql.functions.{count,expr}

  df.groupBy("ID").agg(
    count("AMOUNT").alias("TOTAL_QTY"),
    expr("COUNT(AMOUNT)")
  ).show()


  // window
  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.functions.col
  val windowSpec = Window
    .partitionBy("ID")
    .orderBy(col("NAME").desc)
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  import org.apache.spark.sql.functions.max

  val maxPurchaseQuantity = max(col("AMOUNT")).over(windowSpec).alias("MAX_AMT_BY_ID")

  println(maxPurchaseQuantity)

  df.select(col("ID"),maxPurchaseQuantity).show()

  //rollup and cube

  // pivot
  val pivoted = df.groupBy("ID").pivot("NAME").sum()

  pivoted.show()

  // UDAF -> aggregate function

  import org.apache.spark.sql.expressions.MutableAggregationBuffer
  import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types._
  class BoolAnd extends UserDefinedAggregateFunction {
    def inputSchema: org.apache.spark.sql.types.StructType =
      StructType(StructField("value", BooleanType) :: Nil)
    def bufferSchema: StructType = StructType(
      StructField("result", BooleanType) :: Nil
    )
    def dataType: DataType = BooleanType
    def deterministic: Boolean = true
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = true
    }
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getAs[Boolean](0) && input.getAs[Boolean](0)
    }
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getAs[Boolean](0) && buffer2.getAs[Boolean](0)
    }
    def evaluate(buffer: Row): Any = {
      buffer(0)
    }
  }

  val ba = new BoolAnd
  spark.udf.register("booland", ba)

  spark.range(1)
    .selectExpr("explode(array(TRUE, TRUE, TRUE)) as t")
    .selectExpr("explode(array(TRUE, FALSE, TRUE)) as f", "t")
    .select(ba(col("t")), expr("booland(f)"))
    .show()
}

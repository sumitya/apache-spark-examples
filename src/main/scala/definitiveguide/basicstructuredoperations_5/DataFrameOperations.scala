package definitiveguide.basicstructuredoperations_5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import util.GetAllProperties

object DataFrameOperations extends App {

  import org.apache.spark.sql.types._

  val b = ByteType
  val dc = DecimalType
  val map = MapType
  val struct = StructType
  val field = StructField

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  import org.apache.spark.sql.types.{Metadata, StringType, StructField, StructType}

  val spark = SparkSession
    .builder
    .appName("basic_structured_operations")
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

  df.show()

  // Column and Expr(expressions) in dataframe.

  import org.apache.spark.sql.functions.{col}

  col("testColumn") + 3
  val columnNew = df.select(col("ID") + 5)
  println(columnNew.show())

  import org.apache.spark.sql.functions.expr

  df.select(expr("(ID + 5)/10") + 5).show()
  df.select((expr("(ID + 5)/10") + 5).alias("NEW_ID")).show()
  df.selectExpr("(ID + 5)/10").show()
  df.selectExpr("(ID + 5)/10", "ID as NEW_ID").show()
  df.selectExpr("*", "(ID = 123) as 1_0thID").show()
  df.selectExpr("avg(AMOUNT) as AVG").show()


  // Creating a Row

  import org.apache.spark.sql.Row

  val row = Row("Hello", "World", 10, 1.23, 'c')

  println(row(0))
  println(row(2).asInstanceOf[Int])
  println(row.getString(1))

  //Creating a dataframe

  spark.read.format("csv").load(csvFile).createOrReplaceTempView("CSV_TABLE")

  //Creating a DataFrame from a Row

  val rows = Seq(Row("Hello World", 10, 1.23, "abc"),
    Row("Hi World", 20, 2.23, "pqr")
  )

  val rowSchema = StructType(
    Array(
      StructField("GREET_STR", StringType, true),
      StructField("COUNT", IntegerType, true),
      StructField("PERCENTAGE", DoubleType, true),
      StructField("RANDOM_STR", StringType, true)
      //StructField("CHAR",CharType(1),true)  -> The CharType and VarCharType are there to ensure compatibility
      //with Hive and ORC; they should not be used anywhere else. else they give error.

    )
  )

  val rowRDD = spark.sparkContext.parallelize(rows)
  spark.createDataFrame(rowRDD, rowSchema).show()

  // Literals -> we need to pass explicit values into Spark that are just a value (rather than a new column)
  // lit function converts the native types to Spark types.

  import org.apache.spark.sql.functions.lit

  df.select(expr("*"), lit(1).as("One")).columns.foreach(println)

  // Adding a column to the dataframe -> withColumn("Column_Name",ColumnValue)

  df.withColumn("NewColumn", lit(2)).show()
  df.withColumn("CheckColumn", expr("ID==123")).show()
  df.withColumnRenamed("ID", "Identity").show()

  // NOTE: difference between withColumn , withColumnRenamed -> first adds a new Column,later just renames a existing one
  // we can rename an existing with withColumn but it will be new Column in DataFrame.

  // Refering a column with long column name

  val longColDF = df.withColumn("This is a long Column", lit(1)) // -> it is just a addition, it works.
  longColDF.show()
  longColDF.selectExpr("`This is a long Column`").show() // -> here expr is refering a column, so ` is required here

  // drop a column

  longColDF.drop("This is a long Column").show()

  //changing a data type of a column

  df.withColumn("ID_STR_TYPE", col("ID").cast("string")).printSchema()

  // filtering records

  df.filter("ID > 123").show()
  df.where(col("ID") > 123).show()

  // get samples and Random Split a dataframe

  df.sample(false, .5, 1).show()

  // randomSplit(weights,seed) -> by what percent DF should be split

  val randomSplitDF = df.randomSplit(Array(0.25, 0.25, 0.75), 1)

  println(randomSplitDF.length)

  // Union in DataFrame

  val newRows = Seq(Row(124, "abc", 123.45))
  val newSchema = df.schema

  val parallelizedRows = spark.sparkContext.parallelize(newRows)
  val newDF = spark.createDataFrame(parallelizedRows, newSchema)

  val unionedDF = df.union(newDF)
  unionedDF.union(newDF).where("ID !=123").show()

  // sort and orderBy in DataFrame -> orderBy internally calls sort function only, both functions perform the same way.

  import org.apache.spark.sql.functions.{desc, asc}

  unionedDF.sort(expr("ID").desc).show()
  unionedDF.sort(col("ID").asc_nulls_first).show() // -> it will in asc order with nulls appear first
  unionedDF.sort(desc("ID"), asc("NAME")).show()
  unionedDF.sort(desc("ID"), asc("NAME")).show()
  unionedDF.orderBy(desc("ID"), asc("NAME")).show()

  unionedDF.sortWithinPartitions("ID").show() // -> it is optimized sort, sort within each partition before another set of transformations

  //Repartition and Coalesce -> Repartition will incur a full shuffle of the data, regardless of whether one is necessary.
  //Coalesce, on the other hand, will not incur a full shuffle and will try to combine partitions.

  println(df.rdd.getNumPartitions)
  println(df.repartition(5).rdd.getNumPartitions)
  println(df.repartition(5, col("ID")).rdd.getNumPartitions)
  println(df.repartition(5).coalesce(2).rdd.getNumPartitions)

  // all the below actions will bring data to driver

  val collectDF = df.limit(5)
  collectDF.take(5) // take works with an Integer count
  collectDF.show() // this prints it out nicely
  collectDF.show(4, false)
  collectDF.collect()

  // spark catalog -> it maintains the catalog of current table and columns in a user application.
  // catalyst optimizer does refer the catalog only to resolved the unresolved logical plan.

  println(spark.catalog.currentDatabase)
  println(spark.catalog.listTables().show())

  spark.sqlContext.sql("show functions")
  spark.sqlContext.sql("show system FUNCTIONS")
}

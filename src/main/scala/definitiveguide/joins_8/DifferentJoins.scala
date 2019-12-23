package definitiveguide.joins_8

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DifferentJoins extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession
    .builder
    .appName("DifferentJoins")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  val person = Seq(
    (0, "Bill Chambers", 0, Seq(100)),
    (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
    (2, "Michael Armbrust", 1, Seq(250, 100)))
    .toDF("id", "name", "graduate_program", "spark_status")
  val graduateProgram = Seq(
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley"))
    .toDF("id", "degree", "department", "school")
  val sparkStatus = Seq(
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor"))
    .toDF("id", "status")


  person.createOrReplaceTempView("person")
  graduateProgram.createOrReplaceTempView("graduateProgram")
  sparkStatus.createOrReplaceTempView("sparkStatus")

  val joinExpression = person.col("graduate_program") === graduateProgram.col("id")

  val joinType = "left_semi" // inner,outer,left_outer,right_outer,left_semi,left_anti,cross

  person.join(graduateProgram,joinExpression,joinType).show()  // -> if no join type is provided, default is inner

  import org.apache.spark.sql.functions.expr

  // Joins on Complex Types

  person.withColumnRenamed("id", "personId")
    .join(sparkStatus, expr("array_contains(spark_status, id)")).show()

}




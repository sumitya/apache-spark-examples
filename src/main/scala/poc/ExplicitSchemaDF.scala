package poc

import org.apache.spark.sql.SparkSession

object ExplicitSchemaDF {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Explicit Schema to Dataframe ")
      .config("spark.executor.memory", "1g")
      .config("spark.driver.memory", "1g")
      .master("local")
      .getOrCreate()

    val df = spark.read.csv("src/main/resources/samplerecords_small.csv")

    df.printSchema()

    val df1 = df.toDF("Retailer_country","Order_method_type","Retailer_type","Product_line","Product_type","Product","Year","Quarter","Revenue","Quantity","Gross_margin")

    df1.printSchema()

    val df2 = df1.select("Retailer_country","Order_method_type")

    df2.printSchema()
  }

}

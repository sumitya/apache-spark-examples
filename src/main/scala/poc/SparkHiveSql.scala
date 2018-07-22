package poc

object SparkHiveSql{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.metastore.uris","thrift://localhost:9083")
      .enableHiveSupport()
      .master("local")
      .getOrCreate()

    val df1 = spark.sql("select * from web_analytics.website_users")

    val df2 = spark.sql("select * from web_analytics.page_visitors")

    val df_result = df1.join(df2,Seq("user_id"))

    df_result.printSchema()
    df_result.show()

    df1.createOrReplaceTempView("website_users")
    df2.createOrReplaceGlobalTempView("page_visitors")

    spark.sql("select p1.user_id, p2.page_id from website_users p1 inner join page_visitors p2 where p1.user_id = p2.user_id").show()

  }

}
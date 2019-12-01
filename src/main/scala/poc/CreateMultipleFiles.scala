package poc

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}

object CreateMultipleFiles extends App{

  val sconf = new SparkConf().setAppName("Json").setMaster("local")
  sconf.set("spark.scheduler.mode", "FAIR")
  val sc = new SparkContext(sconf)

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val userName = System.getProperty("user.name")

  val filecsv = sqlContext.read.csv(s"C:\\Users\\${userName}\\intelliJProjects\\apache-spark-examples1\\apache-spark-examples\\src\\main\\resources\\WA_Sales_Products_2012-14.csv")

  filecsv.cache()

  /*println(filecsv.count())
  filecsv.printSchema()
  filecsv.show()
  */


  import org.apache.spark.sql.functions._

  // this will create tasks for every partition. Still it is asynchronous. i. write operaration has to wait for partition generation.
  //filecsv.repartition(10,col("_c9")).write.mode(SaveMode.Overwrite).partitionBy("_c9").csv(s"C:\\Users\\${userName}\\intelliJProjects\\apache-spark-examples1\\apache-spark-examples\\src\\main\\resources\\output\\filecsv")

  // lets fetch the no. of cored and then run those many actions, with condition task - job - core ration should be (1:1:1).
  for(i<-0 to 10){
    // this will create separate jobs i.e. 11 jobs, since it will trigger 11 actions.
    filecsv.write.mode(SaveMode.Overwrite).csv(s"C:\\Users\\${userName}\\intelliJProjects\\apache-spark-examples1\\apache-spark-examples\\src\\main\\resources\\output\\loopoutput\\${i}")

  }



  Thread.sleep(86400000)

  sc.stop()


}

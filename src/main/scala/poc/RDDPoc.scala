package poc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object RDDPoc {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

   //init spark conf and spark context
    val conf = new SparkConf(true).setMaster("local[1]").setAppName("RDDPoc")

    val sc = new SparkContext(conf)

    val data = 1 to 100

    //ways to create RDD

    val fileRDD = sc.textFile("/home/hduser/IdeaProjects/spark2demo/src/resources/WA_Sales_Products_2012-14.csv")

    val listRDD = sc.parallelize(data)

    listRDD.foreach(println)

    //csv file has header to skip header from the data.
    val header = fileRDD.first()

    val dataRDD = fileRDD.filter(row => row != header)

    //map transformation on RDD
    val splittedRDD = dataRDD.map(f => f.split(","))

    val mappedRDD = splittedRDD.map(f => (f(0),f(9).toInt))

    //filter transformation
    val filterRDD = mappedRDD.filter(f => f._1.startsWith("China"))

    //print the RDD lineage graph
    println(filterRDD.toDebugString)

    //iterate or loop through all the rows or Row objects. Here just on 10 rows
    filterRDD.take(10).foreach(println)

    //aggregate transformation
    //val reducedRDD = filterRDD.reduceByKey((a,b) => a + b )

    //Below statement is same as above
    val reducedRDD = mappedRDD.reduceByKey(_ + _ )
    reducedRDD.foreach(println)

    mappedRDD.countByKey().foreach(println)

  }
}

package poc

import java.nio.file.Files
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import util.GetAllProperties

object RDDPoc {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.DEBUG)

   // get input file location var
   var inputFile = GetAllProperties.readPropertyFile get "INPUT_FILE" getOrElse("#")


   //init spark conf and spark context
    val conf = new SparkConf(true).setMaster("local[1]").setAppName("RDDPoc")

    val sc = new SparkContext(conf)

    conf.set("spark.eventLog.enabled","true")

    val data = 1 to 100

    //ways to create RDD
    val fileRDD = sc.textFile(inputFile)

    val listRDD = sc.parallelize(data)

   println("PATH : "+listRDD.getCheckpointFile)



    listRDD.foreach(println)

    //flatMap operation

    val sampleDataRDD = fileRDD.sample(false,.01)
    sampleDataRDD.flatMap(input => input.split(",")).foreach(println)

    //csv file has header to skip header from the data.
    val header = fileRDD.first()

    val dataRDD = fileRDD.filter(row => row != header)

    //map transformation on RDD
    val splittedRDD = dataRDD.map(f => f.split(","))

    val mappedRDD = splittedRDD.map(f => (f(0),f(9).toInt))

    //filter transformation
    val filterRDD = mappedRDD.filter(f => f._1.startsWith("China") || f._1.startsWith("United States") )

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

   //getNumberofPartitions

    println(mappedRDD.getNumPartitions)

    println(mappedRDD.coalesce(2).getNumPartitions)

   sc.stop()
  }

}

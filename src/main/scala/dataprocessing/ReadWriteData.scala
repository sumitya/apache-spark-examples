package dataprocessing

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import util.GetAllProperties

object ReadWriteData extends App{

  Logger.getLogger("org").setLevel(Level.OFF)

  // get input file location var
  val userName = System.getProperty("user.name")

  var smallInputFiles = GetAllProperties.readPropertyFile get "SMALLINPUTFILES" getOrElse ("#") replace("<USER_NAME>", userName)

  var sampleOutputFiles = GetAllProperties.readPropertyFile get "SAMPLEOUTPUTFILES" getOrElse ("#") replace("<USER_NAME>", userName)


  //init spark conf and spark context. the legacy way to initialize sparkContext
  val conf = new SparkConf(true).setMaster("local[1]").setAppName("RDDPoc")
  val sc = new SparkContext(conf)


  val wholeTextFileRDD = sc.wholeTextFiles(smallInputFiles)

  val upperRDD = wholeTextFileRDD.map{
    row => (row._1.toUpperCase,row._2)
  }

  upperRDD.take(20).foreach(println)


  val mappedValues =  upperRDD.mapValues{
    y =>
      (y.split(",")(2),1)
  }.take(20).foreach(println)


  sc.parallelize(List(("Panda", 3), ("Kay", 6), ("Snail", 2))).saveAsSequenceFile(sampleOutputFiles)

  sc.parallelize(List(("Panda", 3), ("Kay", 6), ("Snail", 2))).saveAsObjectFile(sampleOutputFiles)


  Thread.sleep(200000000)

  sc.stop()
}

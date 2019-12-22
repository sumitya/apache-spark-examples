package poc

import org.apache.spark.sql.SparkSession
import util.GetAllProperties

object CreateBroadCast extends App{

  //init spark conf and spark context. the legacy way to initialize sparkContext
 /* val conf = new SparkConf(true).setMaster("local[1]").setAppName("CreateAccumulator")
  val sc = new SparkContext(conf)
*/
  val spark = SparkSession
    .builder
    .appName("CreateAccumulator")
    .master("local[1]")
    .getOrCreate()

  val userName = System.getProperty("user.name")

  var userInfoFile = GetAllProperties.readPropertyFile get "USERINFOFILE" getOrElse ("#") replace("<USER_NAME>", userName)

  var userLinksFile = GetAllProperties.readPropertyFile get "USERLINKSFILE" getOrElse ("#") replace("<USER_NAME>", userName)

  val userInfoRDD = spark.sparkContext.textFile(userInfoFile)

  val userInfoPairRDD = userInfoRDD.map{
    row =>
      val pairs = (row.split(","))
      (pairs(0),pairs(1))
  }

  val userLinksRDD = spark.sparkContext.textFile(userLinksFile)

  val userLinksPairRDD = userLinksRDD.map{
    row =>
      val pairs = (row.split(","))
      (pairs(0),pairs(1))
  }

  val userLinksBroadcasted = spark.sparkContext.broadcast(userLinksPairRDD.collectAsMap())


  val mapSideJoin = userInfoPairRDD.map{

    row =>

      (row._1,userLinksBroadcasted.value.getOrElse(row._1,"No Such Key"))

  }

  mapSideJoin.take(10).foreach(println)

  Thread.sleep(10000000)




}

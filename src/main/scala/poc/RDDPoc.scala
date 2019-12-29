package poc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import custom.UsersPartitioner
import util.GetAllProperties

case class Sales(Retailer_country: String, Order_method: String, `type`: String, Retailer_type: String, Product_line: String, Product_type: String, Product: String, Year: Int, Quarter: String, Revenue: Double, Quantity: Double, Gross_margin: Double)

object RDDPoc {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    // get input file location var
    val userName = System.getProperty("user.name")

    var inputFile = GetAllProperties.readPropertyFile get "INPUT_FILE" getOrElse ("#") replace("<USER_NAME>", userName)

    var userInfoFile = GetAllProperties.readPropertyFile get "USERINFOFILE" getOrElse ("#") replace("<USER_NAME>", userName)

    var userLinksFile = GetAllProperties.readPropertyFile get "USERLINKSFILE" getOrElse ("#") replace("<USER_NAME>", userName)

    //init spark conf and spark context. the legacy way to initialize sparkContext
    val conf = new SparkConf(true).setMaster("local[1]").setAppName("RDDPoc")
    val sc = new SparkContext(conf)

    conf.set("spark.eventLog.enabled", "true")

    val data = 1 to 100

    val fisrtPairData = List((10, 20), (30, 40), (30, 60))

    val secondPairData = List((5,10),(15,20),(15,30))

    val nameValue = Seq(("Panda",1),("Panda",2),("Kang",2),("Fu",3),("Kang Fu Panda",4))

    val stringOfWords = Seq("This is is a kang kang fu fu fu panda panda")

    //ways to create RDD
    val fileRDD = sc.textFile(inputFile)

    fileRDD.cache()

    val listRDD = sc.parallelize(data)

    val pairRDD1 = sc.parallelize(fisrtPairData)

    val pairRDD2 = sc.parallelize(secondPairData)

    val nameValueRDD = sc.parallelize(nameValue)

    val stringofWordsRDD = sc.parallelize(stringOfWords)

    checkType(pairRDD1)
    checkType(fisrtPairData)
    println(pairRDD1.getClass.getName)

    import scala.reflect.runtime.universe._

    def checkType[T](rdd:T)(implicit type1:TypeTag[T]): Unit ={
      println(type1.tpe.typeArgs)
    }


    //keyPairRDDOperation

    //twoKeyPairRDDOperation

    //aggregateRDDOperation

    //wordCount

    //joinCoGroupRDD

    //zipRDDs

    userPartitioning

    def registerToKyro = {

      // You can register your class with Kyro Serializer like below::

      case class Test1(a:Int)
      case class Test2(b:Int)
      conf.registerKryoClasses(Array(classOf[Test1], classOf[Test2]))
      val sc = new SparkContext(conf)
    }


    def userPartitioning = {

      // NOTE::
      // Coalesce -> coalesce effectively collapses partitions on the same worker in order to avoid a shuffle of the data when repartitioning
      // Repartition -> The repartition operation allows you to repartition your data up or down but performs a shuffle across nodes in the process

      val userInfoRDD = sc.textFile(userInfoFile)

      println(userInfoRDD.map(line => (line.split(",")(0), line)).partitionBy(new UsersPartitioner).getNumPartitions)
      println(userInfoRDD.map(line => (line.split(",")(0), line)).getNumPartitions)


    }

    def zipRDDs = {

      // zip allows you to “zip” together two RDDs, assuming that they have the same length.
      // This creates a PairRDD. The two RDDs must have the same number of partitions as well
      // as the same number of elements.

      val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
        .split(" ")
      val words = sc.parallelize(myCollection, 2)

      val numrange = sc.parallelize(0 to 9, 2)

      words.zip(numrange).collect().foreach(println)


    }

    def joinCoGroupRDD = {

      val userInfoRDD = sc.textFile(userInfoFile)

      println(userInfoRDD.partitioner)

      val pairUserInfoRDD = userInfoRDD.map(line => (line.split(",")(0),line)).partitionBy(new HashPartitioner(100)).persist()
      println(pairUserInfoRDD.partitioner)

      val userLinksRDD = sc.textFile(userLinksFile)
      val pairuserLinksRDD = userLinksRDD.map(line => (line.split(",")(0),line))

      //for join operation you need
      val joined = pairUserInfoRDD.join(pairuserLinksRDD)
      println(joined.partitioner)

      joined.take(10).foreach(println)

      val sameDomainKeyWord = joined.filter{
        case(userid,joineddata) => joineddata._2.contains("Scala.abc.com")
      }

      println(sameDomainKeyWord.partitioner)

      println("This user visited to the same domain Name: "+sameDomainKeyWord)

      //CoGroups give you the ability to group together up to three key–value RDDs together in Scala and two in Python
      val cogroupedRDD = pairUserInfoRDD.cogroup(pairuserLinksRDD,pairUserInfoRDD)

      cogroupedRDD.take(10).foreach(println)

    }


    def wordCount = {

      stringofWordsRDD.flatMap( str => str.split(" ")).map(x => (x,1)).countByValue().foreach(println)

      stringofWordsRDD.flatMap( str => str.split(" ")).map(x => (x,1)).reduceByKey((v1,v2) =>v1 + v2).foreach(println)

      stringOfWords.flatMap(str => str.split(" "))

      fileRDD.flatMap(str => str.split(" ")).countByValue().foreach(println)

    }

    def keyPairRDDOperation = {



      val reduceByKey = pairRDD1.reduceByKey((x,y) => x + y)

      reduceByKey.foreach(println)
      println("===========================")

      val groupbykey = pairRDD1.groupByKey()
      println(groupbykey.partitioner)

      groupbykey.foreach(println)
      println("===========================")

      //Apply a function to each value of a pair RDD without changing the key
      pairRDD1.mapValues(x => x *2).partitioner.foreach(println)
      //println(groupbykey.partitioner)

      println("===========================")

      // Pass each value in the key-value pair RDD through a flatMap function without changing the
      // keys; this also retains the original RDD's partitioning.
      pairRDD1.flatMapValues(x => (x to 100)).foreach(println)
      println("===========================")

      pairRDD1.keys.foreach(println)
      println("===========================")
      pairRDD1.values.foreach(println)
      println("===========================")
      pairRDD1.sortByKey().foreach(println)
      println("===========================")


      // nameValueRDD: Seq(("Panda",1),("Panda",2),("Kang",2),("Fu",3),("Kang Fu Panda",4))
      // mapValues: ("Panda",(1,1)),,("Panda",(2,1)),("Kang",(2,1)),("Fu",(3,1)),("Kang Fu Panda",(4,1))
      // reduceBykey: (Kang,(2,1)) (Kang Fu Panda,(4,1)) (Panda,(3,2)) (Fu,(3,1))

      val nameValueReduceBykey = nameValueRDD.mapValues( x => (x,1)).reduceByKey((x,y) => (x._1 + y._1, x._1+y._2))

      nameValueReduceBykey.foreach(println)

    }

    def twoKeyPairRDDOperation = {

      val fisrtPairData = List((10, 20), (30, 40), (30, 60))

      val secondPairData = List((5,10),(15,20),(15,30))

      // Remove elements with a key present in the other RDD.
      pairRDD1.subtractByKey(pairRDD2).foreach(println)
      println("===========================")

      pairRDD1.join(pairRDD2).foreach(println)
      println("===========================")

      pairRDD1.leftOuterJoin(pairRDD2).foreach(println)
      println("===========================")

      pairRDD1.rightOuterJoin(pairRDD2).foreach(println)
      println("===========================")

      pairRDD1.cogroup(pairRDD2).foreach(println)

    }

    def aggregateRDDOperation = {

      val header = fileRDD.first()

      val filteredRDD = fileRDD.filter(row => row != header)

      val mapped = filteredRDD.map {
        line => line.split(",")

      }

      val tupleRDD = filteredRDD.map(element => (element(6), element(8)))

      val revenueRDD = filteredRDD.map(element => element(8).toInt)

      // reduce function can do arthematic operation on single column RDD.

      val totalRevenue = revenueRDD.reduce(_ + _)

      println(totalRevenue)

      // fold is same as reduce with the initial value i.e. zerovalue used for calculation.
      println(revenueRDD.fold(0)(_ + _))

      // FOLDBYKEY -> foldByKey merges the values for each key using an associative function and
      // a neutral “zero value,” which can be added to the result an arbitrary number of times, and must not change the result

      def sumFunc = (v1: Int, v2: Int) => v1 + v2

      filteredRDD.map(element => (element(8).toInt,element(9).toInt)).foldByKey(0)(sumFunc)

      val aggregateRevenue = tupleRDD.aggregate(1)(

        /*
     |     * This is a seqOp for merging T into a U
     |     * ie (String, Int) in  into Int
     |     * (we take (String, Int) in 'value' & return Int)
     |     * Arguments :
     |     * acc   :  Reprsents the accumulated result
     |     * value :  Represents the element in 'inputrdd'
     |     *          In our case this of type (String, Int)
     |     * Return value
     |     * We are returning an Int

        Partition 1 : Sum(all Elements) + 1 (Zero value)
        Partition 2 : Sum(all Elements) + 1 (Zero value)
        Partition 3 : Sum(all Elements) + 1 (Zero value)

     |     */


        (acc, value) => (acc + value._2),


        /*
    |     * This is a combOp for mergining two U's
    |     * (ie 2 Int)

        Partition1 + Partition2 + Partition3 + 3(Zero value) -> combine operation happens at driver.
    |     */

        (acc1, acc2) => (acc1 + acc2)
      )

      println(aggregateRevenue)

      def sumComputation = (v1: Int, v2: Int) => v1 * 10

      tupleRDD.take(2).foreach(println)

      revenueRDD.treeAggregate(1)(sumComputation,sumComputation,2)

        //treeAggregate that does the same thing as aggregate (at the user level) but does so in a different way.
        // It basically “pushes down” some of the subaggregations (creating a tree from executor to executor)
        // before performing the final aggregation on the driver. Having multiple levels can help you to ensure that the driver does
        // not run out of memory in the process of the aggregation.

      def addComputation = (v1: Int, v2: Char) => v1 * 10

      tupleRDD.aggregateByKey(0)(addComputation,sumComputation) // -> This function does the same as aggregate but instead of doing it partition by partition, it does it by key.

      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      //Calculate the moving average
      val customers = sc.parallelize(List(("Alice", "2016-05-01", 50.00),
        ("Alice", "2016-05-03", 45.00),
        ("Alice", "2016-05-04", 55.00),
        ("Bob", "2016-05-01", 25.00),
        ("Bob", "2016-05-04", 29.00),
        ("Bob", "2016-05-06", 27.00))).
        toDF("name", "date", "amountSpent")

      // Import the window functions.
      import org.apache.spark.sql.expressions.Window
      import org.apache.spark.sql.functions._

      // Create a window spec.
      val wSpec1 = Window.partitionBy("name").orderBy("date").rowsBetween(-1, 1)

      // Calculate the moving average
      customers.withColumn("movingAvg",
        avg(customers("amountSpent")).over(wSpec1)).show()


      //Cumulative Sum.

      //let us calculate the cumulative sum of the amount spent per customer.


      // Window spec: the frame ranges from the beginning (Long.MinValue) to the current row (0).
      val wSpec2 = Window.partitionBy("name").orderBy("date").rowsBetween(Long.MinValue, 0)

      // Create a new column which calculates the sum over the defined window frame.
      customers.withColumn("cumSum",
        sum(customers("amountSpent")).over(wSpec2)).show()

      //Data from previous row. we want to see the amount spent by the customer in their previous visit.

      // Window spec. No need to specify a frame in this case.
      val wSpec3 = Window.partitionBy("name").orderBy("date")

      // Use the lag function to look backwards by one row.
      customers.withColumn("prevAmountSpent",
        lag(customers("amountSpent"), 1).over(wSpec3)).show()

      //Rank, The rank function returns what we want.
      customers.withColumn("rank", rank().over(wSpec3)).show()

    }

    def listRDDOperation = {

      println("PATH : " + listRDD.getCheckpointFile)

      listRDD.foreach(println)

    }

    def fileRDDOperation = {
      val sampleDataRDD = fileRDD.sample(false, .01)
      //flatMap operation

      sampleDataRDD.flatMap(input => input.split(",")).foreach(println)

      //csv file has header to skip header from the data.
      val header = fileRDD.first()

      val dataRDD = fileRDD.filter(row => row != header)

      //map transformation on RDD
      val splittedRDD = dataRDD.map(f => f.split(","))

      val mappedRDD = splittedRDD.map(f => (f(0), f(9).toInt))

      //filter transformation
      val filterRDD = mappedRDD.filter(f => f._1.startsWith("China") || f._1.startsWith("United States"))

      //print the RDD lineage graph
      println(filterRDD.toDebugString)
      //iterate or loop through all the rows or Row objects. Here just on 10 rows
      filterRDD.take(10).foreach(println)

      //aggregate transformation
      //val reducedRDD = filterRDD.reduceByKey((a,b) => a + b )

      //Below statement is same as above
      val reducedRDD = mappedRDD.reduceByKey(_ + _)
      reducedRDD.foreach(println)

      mappedRDD.countByKey().foreach(println)

      //getNumberofPartitions

      println(mappedRDD.getNumPartitions)

      println(mappedRDD.coalesce(2).getNumPartitions)


    }

    Thread.sleep(200000000)

    sc.stop()
  }

}


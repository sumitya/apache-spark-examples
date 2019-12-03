package poc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import util.GetAllProperties

case class Sales(Retailer_country: String, Order_method: String, `type`: String, Retailer_type: String, Product_line: String, Product_type: String, Product: String, Year: Int, Quarter: String, Revenue: Double, Quantity: Double, Gross_margin: Double)

object RDDPoc {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    // get input file location var
    val userName = System.getProperty("user.name")

    var inputFile = GetAllProperties.readPropertyFile get "INPUT_FILE" getOrElse ("#") replace("<USER_NAME>", userName)

    println(inputFile)

    //init spark conf and spark context. the legacy way to initialize sparkContext
    val conf = new SparkConf(true).setMaster("local[1]").setAppName("RDDPoc")
    val sc = new SparkContext(conf)

    conf.set("spark.eventLog.enabled", "true")

    val data = 1 to 100

    //ways to create RDD
    val fileRDD = sc.textFile(inputFile)

    println(fileRDD.getCheckpointFile)
    println(fileRDD.getNumPartitions)

    fileRDD.cache()

    val listRDD = sc.parallelize(data)

    aggregateRDDOperation

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

        Partition1 + Partition2 + Partition3 + 3(Zero value)
    |     */

        (acc1, acc2) => (acc1 + acc2)
      )


      println(aggregateRevenue)


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

    sc.stop()
  }

}


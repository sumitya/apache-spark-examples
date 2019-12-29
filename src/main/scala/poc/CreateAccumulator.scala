package poc

import org.apache.spark.sql.{SparkSession}
import util.GetAllProperties

object CreateAccumulator extends App {

  val spark = SparkSession
    .builder
    .appName("CreateAccumulator")
    .master("local[1]")
    .getOrCreate()

  val userName = System.getProperty("user.name")

  var userinfofile = GetAllProperties.readPropertyFile get "USERINFOFILE" getOrElse ("#") replace("<USER_NAME>", userName)

  val file = spark.sparkContext.textFile(userinfofile)

  //val scalaWords = spark.sparkContext.accumulator(0,"TestAccumulator")

  import org.apache.spark.util.LongAccumulator

  // UnNamed Accumulators -> they are not visible in SparkUI.

  val accUnnamed = new LongAccumulator()

  // Named Accumulators -> they are visible in SparkUI.
  val longAccumulator = spark.sparkContext.longAccumulator("TestLongAccumulator")

  spark.sparkContext.register(accUnnamed,"TestLongAccumulator")

  var testLongVal = 0

 /* val counts = file.map {
    row =>
      testLongVal += 1
      if (row.contains("Scala")) {
        longAccumulator.add(testLongVal.toLong)
      }
  }
*/

  def updateAccum(row:String) = {

    if(row.contains("Scala")){
      testLongVal += 1
      accUnnamed.add(testLongVal.toLong)
    }

  }

  file.foreach( line => updateAccum(line))

  println("Scala Count is: "+accUnnamed.value)

  Thread.sleep(10000000)

}

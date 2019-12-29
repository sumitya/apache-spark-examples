package custom

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2
import util.GetAllProperties

class EvenAccumulator extends AccumulatorV2[BigInt, BigInt]{

  private var num:BigInt = 0

  override def value: BigInt = {this.num}

  override def isZero: Boolean = {
    this.num == 0
  }

  override def copy(): AccumulatorV2[BigInt, BigInt] = {

    new EvenAccumulator
  }

  override def reset(): Unit = {
        this.num=0
  }

  override def add(v: BigInt): Unit = {
    if(v % 2 == 0){

      this.num+=v

    }
  }

  override def merge(other: AccumulatorV2[BigInt, BigInt]): Unit = {

    this.num+=other.value
  }

}
object EvenAccumulator extends  App{

  val spark = SparkSession
    .builder
    .appName("EvenAccumulator")
    .master("local[1]")
    .getOrCreate()

  val acc = new EvenAccumulator

  spark.sparkContext.register(acc, "EvenAcc")

  val userName = System.getProperty("user.name")

  var userinfofile = GetAllProperties.readPropertyFile get "USERINFOFILE" getOrElse ("#") replace("<USER_NAME>", userName)

  case class USER(userId:Int,skill:String)

  import spark.implicits._

  import org.apache.spark.sql.Encoders

  val schema = Encoders.product[USER].schema

  val file = spark.read.format("csv").schema(schema).load(userinfofile).as[USER]

  // -> here as[USER] converts it to dataset, makes it possible to refer it with column names like: row.userId

  file.show()

  file.printSchema()

  file.foreach( row => acc.add(row.userId))

  println(acc.value)

  Thread.sleep(10000000)

  spark.stop()
}




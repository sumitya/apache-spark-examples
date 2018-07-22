package dataprocessing

case class Data(day: Int, doy:Int, month: Int, year: Int,preicpt:Double, snow:Double)
object ReadRawData {

  def main(args: Array[String]): Unit = {
    val source = scala.io.Source.fromFile("/home/hduser/IdeaProjects/sparkpoc/src/main/resources/MN212142_9392.csv")
    val lines = source.getLines().drop(1)

    val filtereddata = lines.filterNot(_.contains(",.,"))

    val rawdata= filtereddata.map{
      line => val p = line.split(",")
        Data(p(0).toInt,p(1).toInt,p(2).toInt,p(4).toInt,p(5).toDouble,p(6).toDouble)

    }.toArray

    rawdata.foreach(println)

    //source is an iterator so need to close it
      source.close()
  }

}

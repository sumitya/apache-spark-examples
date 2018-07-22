package dataprocessing

case class RawData(day: Int, doy:Int, month: Int, year: Int,preicpt:Double, snow:Double)

object FlatMapRawData {

  def toDoubleFunction(s:String): Double = {

    try{
    s.toDouble
  }catch{
      case _:NumberFormatException => -1
  }
  }


  def main(args: Array[String]): Unit = {
    val source = scala.io.Source.fromFile("/home/hduser/IdeaProjects/sparkpoc/src/main/resources/MN212142_9392.csv")
    val lines = source.getLines().drop(1)

    val rawdata= lines.map{

      line =>

        val p = line.split(",")
        if(p(6)==".") Seq.empty
        else
        Seq(RawData(p(0).toInt,p(1).toInt,p(2).toInt,p(4).toInt,toDoubleFunction(p(5)),(6).toDouble))

    }.toArray


    rawdata.foreach(println)
    println(rawdata.length)

  }

}

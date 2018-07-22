package dataprocessing

import dataprocessing.FlatMapRawData.toDoubleFunction

case class RawInputData(day: Int, doy:Int, month: Int, year: Int,preicpt:Double, snow:Double)

object AverageByYear {

  def main(args: Array[String]): Unit = {
    val source = scala.io.Source.fromFile("/home/hduser/IdeaProjects/sparkpoc/src/main/resources/MN212142_9392.csv")
    val lines = source.getLines().drop(1)


    val rawdata= lines.flatMap{

      line =>

        val p = line.split(",")
        if(p(6)==".") Seq.empty
        else
          Seq(RawData(p(0).toInt,p(1).toInt,p(2).toInt,p(4).toInt,toDoubleFunction(p(5)),(6).toDouble))

    }.toArray

    val countdata = rawdata.count(_.preicpt > 1.0)


    val groupeddata = rawdata.groupBy( element => element.year)


    val aggregateRawData = groupeddata.map{

      case(n,days) =>
        n -> days.foldLeft(0.0)((sum,td) => sum+td.preicpt)/days.length


    }

    aggregateRawData.toSeq.sortBy(_._1).foreach(println)
  }

}

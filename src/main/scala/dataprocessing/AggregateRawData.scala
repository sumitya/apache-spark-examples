package dataprocessing

import dataprocessing.FlatMapRawData.toDoubleFunction

object AggregateRawData {

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


    val max_year = rawdata.map(_.year).max

    //rawdata.foreach(println)
    println(max_year)

    val max_year_records = rawdata.filter(_.year == max_year)

    println("============================")
    max_year_records.foreach(println)

    val maxpreicpt = rawdata.maxBy(_.preicpt)

    println("============================")
    println(maxpreicpt)

    val countdata = rawdata.count(_.preicpt > 1.0)

    println("============================")
    println(countdata)

  }

}

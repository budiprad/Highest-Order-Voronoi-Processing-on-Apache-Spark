import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object Main2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf: SparkConf =
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("CalonTASvaha")
        .set("spark.driver.host", "localhost")
        .set("spark.executor.instances","8")
        .set("spark.executor.cores","8")

    val spark = SparkSession.builder().config(conf).config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse").getOrCreate()

    val dataset = spark.read.option("header",true).csv("coba.txt")

    //convert dataframe to array
    val titikArray = dataset.collect()
    var incrementPointName = 1
    var pointListBuffer = new ListBuffer[Point]()
    //iterate through array, add element to pointList
    for(titik <- titikArray){
      println(titik.getString(0))
      println(titik.getString(1)+" dijadiin double : "+titik.getString(1).toDouble)
      var titikNya = Point(titik.getString(0), titik.getString(1).toDouble, titik.getString(2).toDouble)
      pointListBuffer += titikNya
      incrementPointName +=1
    }

    val algo = new Algorithm2
    algo.assignMinMax(pointListBuffer.toList)
    algo.bisector(pointListBuffer.toList)
    algo.vertex()

    for(xx <- algo.bisBuffer){
      println(xx.nama)
      println("y="+xx.m_value+"x+"+xx.b_value)
      println("list vertex : ")
      for(vv <- xx.vertex){
        println(vv.nama+ " {"+vv.kordinatX+","+vv.kordinatY+"}")
      }
      println("=======================")
    }

    //convert pointListBuffer to list
//    import spark.implicits._
//    val algo = new Algorithm2
//    algo.assignMinMax(pointListBuffer.toList)
//
//    pointListBuffer.toDF().coalesce(1).write.option("header",true).mode("overwrite").csv("..\\Outputdata\\Points")
//
//    val perp_result = algo.bisector(pointListBuffer.toList)
//    val dfWrite = perp_result.toDF()
//    dfWrite.coalesce(1).write.option("header",true).mode("overwrite").csv("..\\Outputdata\\Bisector")
//
//    //CALCULATE INTERSECTION
//    val intersection_result = algo.vertex(perp_result)
//    val intrsectWrite = intersection_result.toDF()
//    intrsectWrite.coalesce(1).write.option("header",true).mode("overwrite").csv("..\\Outputdata\\Vertex")
//    println("banyak intersection :"+intersection_result.size)



  }

}

import org.apache.log4j.{Level, Logger}
import org.apache.spark
import spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{max, min}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Sorting
import scala.math._
import scala.math.Ordering.Implicits._
import scala.concurrent.ExecutionContext.Implicits._

//http://192.168.145.2:4040 --> Spark WEB UI

object Main {
  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.WARN)
//    val spark = SparkSession.builder().master("local[2]")
//      .appName("CalonTASvaha")
//      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
//      .getOrCreate


    val conf: SparkConf =
      new SparkConf()
        .setMaster("local[2]")
        .setAppName("CalonTASvaha")
        .set("spark.driver.host", "localhost")

    val spark = SparkSession.builder().config(conf).config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse").getOrCreate()




    val sc: SparkContext = SparkContext.getOrCreate(conf)

    val sqlContext = spark.sqlContext

    val dataset = spark.read.option("header",true).csv("Dataset8.txt")
    dataset.show()

    //convert dataframe to array
    val titikArray = dataset.collect()
    var incrementPointName = 1
    var pointListBuffer = new ListBuffer[Point]()
    //iterate through array, add element to pointList
    for(titik <- titikArray){
      var titikNya = Point("Point"+incrementPointName, titik.getString(1).toDouble, titik.getString(2).toDouble)
      pointListBuffer += titikNya
      incrementPointName +=1
    }
    //convert pointListBuffer to list
    val algo = new Algorithm
    val preNormalizePoint = pointListBuffer.toList
    val pointList = algo.normalizePoints(preNormalizePoint)

    println("=========  PERPENDICULAR BISECTOR  ======")
    print("PointList : "+pointList)
    algo.assignMinMax(preNormalizePoint)
    val perp_result = algo.perpendicularBisector(pointList).toList


    //Used to convert list to Data frame, and write it to Text File
    import spark.implicits._
    val dfWrite = perp_result.toDF()
    dfWrite.write.option("header",true).mode("overwrite").csv("..\\Outputdata\\Bisector")

    //CALCULATE INTERSECTION
    val intersection_result = algo.bisectorIntersection(perp_result).toList
    val intrsectWrite = intersection_result.toDF()
    intrsectWrite.write.option("header",true).mode("overwrite").csv("..\\Outputdata\\Vertex")
    println("banyak intersection :"+intersection_result.size)

    //CALCULATE THE SEGMENT
    import spark.implicits._
    val segmenting_result = algo.segmentForming(perp_result,intersection_result)
    val segmenting_write = segmenting_result.toDF()
    segmenting_write.write.option("header",true).mode("overwrite").csv("..\\Outputdata\\Segment")

    //CALCULATE REGION
    val listOfRegion = algo.regionForming(segmenting_result)
    import spark.implicits._
    val writeTextRegion = listOfRegion.toDF
    writeTextRegion.write.option("header",true).mode("overwrite").csv("..\\Outputdata\\Region")

    //LABELLING
    val listOfLabel = algo.labeling(listOfRegion, segmenting_result, perp_result, pointList)
    import spark.implicits._
    val writeTextLabelling = listOfLabel.toDF()

    import org.apache.spark.sql.functions.udf
    val stringify = udf((vs: Seq[String]) => s"""[${vs.mkString(",")}]""")
    writeTextLabelling.withColumn("label", stringify($"label")).write.option("header",true).mode("overwrite").csv("..\\Outputdata\\Label")
  }
}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

class Algorithm {
  var x_min, x_max, y_min, y_max = BigDecimal(123456.789)
  var adder10 = BigDecimal(5)
  val timesby = 1000

  Logger.getLogger("org").setLevel(Level.WARN)
  val spark = SparkSession.builder().master("local")
    .appName("CalonTASvaha")
    .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
    .getOrCreate

  val sqlContext = spark.sqlContext
//  val conf = new SparkConf().setAppName("CalonTASvaha").setMaster("local")
//  val sc = new SparkContext(conf)

  def assignMinMax(pointList : List[Point]) ={
    val sortedpointX = pointList.sortWith(_.kordinatX > _.kordinatX)
    val sortedpointY = pointList.sortWith(_.kordinatY > _.kordinatY)


    x_max = (sortedpointX.head.kordinatX+adder10)*timesby
    x_min = (sortedpointX.last.kordinatX-adder10)*timesby
    y_max = (sortedpointY.head.kordinatY+adder10)*timesby
    y_min = (sortedpointY.last.kordinatY-adder10)*timesby
  }

  def factorial(n: Int): BigInt = {
    if (n == 0) 1
    else        n * factorial(n-1)
  }

  def normalizePoints(pointList : List[Point]) : List[Point] = {
    val normalizeList = new ListBuffer[Point]()

    for(xx <- pointList){
      val siX = xx.kordinatX*timesby
      val siY = xx.kordinatY*timesby
      val pp = Point(xx.nama, siX, siY)
      normalizeList += pp
    }

    normalizeList.toList
  }



  //FUNCTION USED TO CALCULATE PERPENDICULAR BISECTOR
  def perpendicularBisector(pointList : List[Point]): List[Bisector] ={
    val t1 = System.nanoTime
    //pointer for element in list that will be calculated
    var pointer1=0
    var pointer2 =1

    //Calculate C(2,M) -> M is number of Point
    val factorialDua = factorial(2)
    val factorialM = factorial(pointList.size)
    val factorialMkurangDua = factorial(pointList.size-2)
    val banyakPerpen = factorialM/(factorialDua*(factorialMkurangDua))
    println("Banyak : "+banyakPerpen)


    val bisBuffer = new ListBuffer[Bisector]()

    //Get Combination(2,M) -> M is number of Point
    for (i <- 0 to (banyakPerpen.toInt)-1){
      if(pointer2 == pointList.length){
        pointer1+=1
        pointer2 = pointer1+1
      }

      //Get 2 Point
      val titikPertama = pointList(pointer1)
      val titikKedua = pointList(pointer2)

      //Calculate Mid Point
      val midPointX = (titikKedua.kordinatX+titikPertama.kordinatX)/2
      val midPointY = (titikKedua.kordinatY+titikPertama.kordinatY)/2
      val midPoint = Point("MidPoint"+titikPertama.nama+titikKedua.nama, midPointX,midPointY)

      //Find Slope m1
      val m1 = (titikKedua.kordinatY - titikPertama.kordinatY)/(titikKedua.kordinatX-titikPertama.kordinatX)

      //Find Slope m2
      val m2 = -1/m1
//      val m2xp = (-1*(m2*midPoint.kordinatX))

      //Find b on y=mx+b
      val bVal = (midPoint.kordinatY - (m2*midPoint.kordinatX))

//      println(titikPertama.nama+ " - "+titikKedua.nama
//        + " MidPoint : "+midPoint.nama+"("+midPoint.kordinatX+","+midPoint.kordinatY+")"
//        + " --- Slope : "+m2+" |||||-   Perpendicular Bisector : y = "+m2+"x + ("+bVal+")")
      pointer2 += 1

      var bisectornya = Bisector("Bis_"+(i+1),"BIS", m2, bVal, titikPertama.nama, titikKedua.nama)
      bisBuffer += bisectornya
    }
    println(" ")

    //Add Boundary Line as Bisector
    //Add the x = ...
    var bisBoundX1 = Bisector("Bis_"+(banyakPerpen+1),"BND", 1, x_min, "-", "-")
    var bisBoundX2 = Bisector("Bis_"+(banyakPerpen+2),"BND", 1, x_max, "-", "-")
    //Add the y = ...
    var bisBoundY1 = Bisector("Bis_"+(banyakPerpen+3),"BND", 0, y_min, "-", "-")
    var bisBoundY2 = Bisector("Bis_"+(banyakPerpen+4),"BND", 0, y_max, "-", "-")

    //Add to List
    bisBuffer += bisBoundX1
    bisBuffer += bisBoundX2
    bisBuffer += bisBoundY1
    bisBuffer += bisBoundY2

    val duration = (System.nanoTime - t1) / 1e9d
    println("Durasi Perp Bisector : "+duration)
    println("===========================================================================================")
    return bisBuffer.toList
  }


  //Calculate Intersection
  // JANGAN PAKE FOR LOOP, PAKE WHILE AJA
  //BERARTI DI BISECTORNYA TAMBAHIN STATUS
  def bisectorIntersection(bisectorList : List[Bisector]): List[Vertex] = {
    val t1 = System.nanoTime

    val bisIntersectionBuffer = new ListBuffer[Vertex]()
    var countInertection =1

    //pointer for element in list that will be calculated
    var pointer1=0
    var pointer2 =1

    //Calculate C(2,M) -> M is number of Point
    val factorialDua = factorial(2)
    val factorialM = factorial(bisectorList.size)
    val lengthBis = (bisectorList.length)-2
    val factorialMkurangDua = factorial(lengthBis)
    println(factorial(lengthBis)+"<-- factorialnya -- "+lengthBis+"<-- lengbis --- factorialMkurangDua : "+factorialMkurangDua + "panjang bisector list : "+bisectorList.length)
    val banyakIntersec = factorialM/(factorialDua*factorialMkurangDua)

    for (i <- 1 to banyakIntersec.toInt){
      if(pointer2 == bisectorList.length){
        pointer1+=1
        pointer2 = pointer1+1
      }

      //Get 2 Point
      val bisectorPertama = bisectorList(pointer1)
      val bisectorKedua = bisectorList(pointer2)

      //CHECK IF BIS1 OR BIS2 IS BOUND
      //IF BOUND INTERSECT WITH BOUND
      if(bisectorPertama.tipe.equals("BND") && bisectorKedua.tipe.equals("BND")){
        if(bisectorPertama.m_value.equals(0) && bisectorKedua.m_value.equals(1)){ //Berarti garis y=..
          var intersectPoint = Vertex("Vertex_"+i, bisectorKedua.b_value, bisectorPertama.b_value, bisectorPertama.nama, bisectorKedua.nama)
          bisIntersectionBuffer += intersectPoint
        } else if(bisectorPertama.m_value.equals(1) && bisectorKedua.m_value.equals(0)){ //Berarti garis x=..
          var intersectPoint = Vertex("Vertex_"+i, bisectorPertama.b_value, bisectorKedua.b_value, bisectorPertama.nama, bisectorKedua.nama)
          bisIntersectionBuffer += intersectPoint
        }

        //IF BIS INTERSECT WITH BIS
      } else if(bisectorPertama.tipe.equals("BIS") && bisectorKedua.tipe.equals("BIS")){
        //Calculate intersection X,Y
        val intersect_x = ((bisectorKedua.b_value-bisectorPertama.b_value) / (bisectorPertama.m_value-bisectorKedua.m_value))
        val intersect_y = (bisectorKedua.m_value*intersect_x)+bisectorKedua.b_value

        if(intersect_x <= x_max && intersect_x >= x_min){
          if(intersect_y <= y_max && intersect_y >= y_min){
            var intersectPoint = Vertex("Vertex_"+i, intersect_x, intersect_y, bisectorPertama.nama, bisectorKedua.nama)
            bisIntersectionBuffer += intersectPoint
          }
        }

      } else {
        //IF BIS INTERSECT WITH BOUND
        if(bisectorPertama.tipe.equals("BND")){
          if(bisectorPertama.m_value.equals(0)){ //Y=...
            var vertexX = (bisectorPertama.b_value-bisectorKedua.b_value)/bisectorKedua.m_value
            if(vertexX <= x_max && vertexX >= x_min){
              if(bisectorPertama.b_value <= y_max && bisectorPertama.b_value >= y_min){
                var intersectPoint = Vertex("Vertex_"+i, vertexX, bisectorPertama.b_value, bisectorPertama.nama, bisectorKedua.nama)
                bisIntersectionBuffer += intersectPoint
              }
            }

          } else { //X = ...
            var vertexY = (bisectorPertama.b_value*bisectorKedua.m_value)+bisectorKedua.b_value
            if(bisectorPertama.b_value <= x_max && bisectorPertama.b_value >= x_min){
              if(vertexY <= y_max && vertexY >= y_min){
                var intersectPoint = Vertex("Vertex_"+i, bisectorPertama.b_value, vertexY, bisectorPertama.nama, bisectorKedua.nama)
                bisIntersectionBuffer += intersectPoint
              }
            }
          }

        } else if(bisectorKedua.tipe.equals("BND")){
          if(bisectorKedua.m_value.equals(0)){ //Y=...
            var vertexX = (bisectorKedua.b_value-bisectorPertama.b_value)/bisectorPertama.m_value

            if(vertexX <= x_max && vertexX >= x_min){
              if(bisectorKedua.b_value <= y_max && bisectorKedua.b_value >= y_min){
                var intersectPoint = Vertex("Vertex_"+i, vertexX, bisectorKedua.b_value, bisectorPertama.nama, bisectorKedua.nama)
                bisIntersectionBuffer += intersectPoint
              }
            }
          } else { //X = ...
            var vertexY = (bisectorKedua.b_value*bisectorPertama.m_value)+bisectorPertama.b_value
            if(bisectorKedua.b_value <= x_max && bisectorKedua.b_value >= x_min){
              if(vertexY <= y_max && vertexY >= y_min){
                var intersectPoint = Vertex("Vertex_"+i, bisectorKedua.b_value, vertexY, bisectorPertama.nama, bisectorKedua.nama)
                bisIntersectionBuffer += intersectPoint
              }
            }
          }

        }
      }
      pointer2 += 1
      countInertection += 1
    }

    val duration = (System.nanoTime - t1) / 1e9d
    println("durasi Vertex : "+duration+" banyak vertex : "+bisIntersectionBuffer.size)
    println("===========================================================================================")
    return bisIntersectionBuffer.toList
  }


//  def findVertex(bisectorList : List[Bisector]) : List[Vertex] = {
//    val bisLength = bisectorList.length
//    var countStatus = 1
//
//    //pointer for element in list that will be calculated
//    var pointer1=0
//    var pointer2 =1
//
//
//    while(countStatus <= bisLength){
//
//      if(pointer2 == bisectorList.length){
//        pointer1+=1
//        pointer2 = pointer1+1
//      }
//
//      //Get 2 Point
//      val bisectorPertama = bisectorList(pointer1)
//      val bisectorKedua = bisectorList(pointer2)
//
//      countStatus += 1
//    }
//
//  }

  def segmentForming(bisectorList : List[Bisector], vertexList : List[Vertex]): List[Segment] ={
    val t1 = System.nanoTime
//
//    val vertexFull = spark.read.option("header",true).option("inferSchema", "true").csv("..\\Outputdata\\Vertex")
//    val tempTable = vertexFull.createOrReplaceTempView("vertex")

    import spark.sqlContext.implicits._
    var vertexFull = vertexList.toDF("nama","kordinatX","kordinatY","bisector1","bisector2").cache()
    var tempTable = vertexFull.createOrReplaceTempView("vertex")

    val segmentFull = new ListBuffer[Segment]()
    val banyakLoopBis = bisectorList.size
    var countSeg = 1
    println("Banyak loopbis : "+banyakLoopBis)
    for (i <- 0 to banyakLoopBis-1) {
      val bisector = bisectorList(i)
      println(bisector.nama+"...")

      if(bisector.m_value == 1){
        val t2 = System.nanoTime
        val preusingSQL = sqlContext.sql("select * from vertex where bisector1 = '"+bisector.nama+"' or bisector2 = '"+bisector.nama+"' order by kordinatY ASC")
        val arrCol = Array("kordinatX", "kordinatY")
        val usingSQL = preusingSQL.dropDuplicates(arrCol)

        val vvv = usingSQL.as[Vertex].collect().to[ListBuffer]
        for(vi <- 0  to vvv.length-1){
          val va = vi+1
          if(va < vvv.length){
            val segmentnya = Segment("Seg_"+countSeg,vvv(vi).kordinatX,vvv(vi).kordinatY, vvv(va).kordinatX, vvv(va).kordinatY, bisector.nama, bisector.tipe,0)
            countSeg += 1
            val segmentnya2 = Segment("Seg_"+countSeg,vvv(va).kordinatX,vvv(va).kordinatY, vvv(vi).kordinatX, vvv(vi).kordinatY, bisector.nama, bisector.tipe,0)
            segmentFull += segmentnya
            segmentFull += segmentnya2
            countSeg += 1
          }

        }

      } else {
        val preusingSQL = sqlContext.sql("select * from vertex where bisector1 = '"+bisector.nama+"' or bisector2 = '"+bisector.nama+"' order by kordinatX ASC")
        val arrCol = Array("kordinatX", "kordinatY")
        val usingSQL = preusingSQL.dropDuplicates(arrCol)
        val vvv = usingSQL.as[Vertex].collect().to[ListBuffer]
        for(vi <- 0  to vvv.length-1){
          val va = vi+1
          if(va < vvv.length){
            val segmentnya = Segment("Seg_"+countSeg,vvv(vi).kordinatX,vvv(vi).kordinatY, vvv(va).kordinatX, vvv(va).kordinatY, bisector.nama, bisector.tipe,0)
            countSeg += 1
            val segmentnya2 = Segment("Seg_"+countSeg,vvv(va).kordinatX,vvv(va).kordinatY, vvv(vi).kordinatX, vvv(vi).kordinatY, bisector.nama, bisector.tipe,0)
            segmentFull += segmentnya
            segmentFull += segmentnya2
            countSeg += 1
          }
        }

      }


    }
    val duration = (System.nanoTime - t1) / 1e9d
    println("durasi cari segment : "+duration + "banyak segment : "+countSeg)
    println("===========================================================================================")
    return segmentFull.toList
  }

  ///------------ BARUUUU

  def getAngle(segAwal : Segment, segCand : Segment): Double ={

    val angle = math.atan2((segAwal.Y_awal-segAwal.Y_akhir).toDouble, (segAwal.X_awal-segAwal.X_akhir).toDouble) - math.atan2((segCand.Y_akhir-segAwal.Y_akhir).toDouble, (segCand.X_akhir-segAwal.X_akhir).toDouble);
    var angleDeg = angle * 360 / (2*math.Pi)
    if (angleDeg < 0.0)
      angleDeg += 360
    angleDeg
  }

  def regionForming(segList : List[Segment]): List[Region] ={
    val t1 = System.nanoTime
    println("Processing Region...")
    val segmentFull = new ListBuffer[Segment]()
    val regList = new ListBuffer[Region]()
    var segListBuffer = segList.to[ListBuffer]



    var isSisaSegment = false
    var regionCount =1;
    while(isSisaSegment == false){
      println("Processing - R"+regionCount)

      import spark.sqlContext.implicits._
      val bval = spark.sparkContext.broadcast(segListBuffer)
      var segmentDF = bval.value.toDF("nama","X_awal","Y_awal","X_akhir","Y_akhir","bisector","tipe","usage")
      var tempTable = segmentDF.createOrReplaceTempView("segment")

      val dataSegmentStart = sqlContext.sql("select * from segment where tipe = 'BIS' and usage = 0").first()

      var X_begin = BigDecimal(dataSegmentStart.getDecimal(1))
      var Y_begin = BigDecimal(dataSegmentStart.getDecimal(2))

      var segmentStart = Segment(dataSegmentStart.getString(0), dataSegmentStart.getDecimal(1), dataSegmentStart.getDecimal(2)
        , dataSegmentStart.getDecimal(3), dataSegmentStart.getDecimal(4), dataSegmentStart.getString(5), dataSegmentStart.getString(6), dataSegmentStart.getInt(7))
//      println("Segment yang diproses selanjutnya:::: "+segmentStart)

      val indexSegPemula = segListBuffer.indexOf(segmentStart)

      segListBuffer(indexSegPemula).usage=1
//      println(indexSegPemula+"<--- Nomor index : Hasil setelah diubah --> "+segListBuffer(indexSegPemula))

      var regStart = Region("R"+regionCount, segmentStart.X_awal, segmentStart.Y_awal, segmentStart.X_akhir, segmentStart.Y_akhir, segmentStart.tipe,0)
      regList += regStart

      while(!(X_begin.equals(segmentStart.X_akhir) && Y_begin.equals(segmentStart.Y_akhir))){
        val getNextSegment = sqlContext.sql("select * from segment where X_awal="+segmentStart.X_akhir+" and Y_awal="+segmentStart.Y_akhir+" and bisector != '"+segmentStart.bisector+"' and usage < 1")
        val getNextSegmentData = getNextSegment.coalesce(1).collect()
        var sudut = 360.0
        println(" CALON SEGMENT : R"+regionCount)
        getNextSegment.show()

        var nextSegName, nextSegBis, nextSegTipe = ""
        var nextX_awal, nextY_awal, nextX_akhir, nextY_akhir = BigDecimal(0.0)
        var nextUsage = 0

        for(dataNextSegment <- getNextSegmentData){
          val segTarget = Segment(dataNextSegment.getString(0),dataNextSegment.getDecimal(1),dataNextSegment.getDecimal(2),dataNextSegment.getDecimal(3),dataNextSegment.getDecimal(4),dataNextSegment.getString(5),dataNextSegment.getString(6),dataNextSegment.getInt(7))
          var curr_sudut = getAngle(segmentStart,segTarget)
//          println(segmentStart.nama+" dengan "+ dataNextSegment.getString(0)+" sudutnya : "+(curr_sudut))
          if(curr_sudut <= sudut){
            sudut = curr_sudut
            nextSegName = dataNextSegment.getString(0)
            nextX_awal = dataNextSegment.getDecimal(1)
            nextY_awal = dataNextSegment.getDecimal(2)
            nextX_akhir = dataNextSegment.getDecimal(3)
            nextY_akhir = dataNextSegment.getDecimal(4)
            nextSegBis = dataNextSegment.getString(5)
            nextSegTipe = dataNextSegment.getString(6)
            nextUsage = dataNextSegment.getInt(7)
          }
        }
//        println("Yang dipilih : "+nextSegName)
        segmentStart.nama = nextSegName
        segmentStart.X_awal = nextX_awal
        segmentStart.Y_awal = nextY_awal
        segmentStart.X_akhir = nextX_akhir
        segmentStart.Y_akhir = nextY_akhir
        segmentStart.bisector = nextSegBis
        segmentStart.tipe = nextSegTipe
        segmentStart.usage = nextUsage

        var xTest = segmentStart.X_akhir
        var yTest = segmentStart.Y_akhir

        val segPilihan = Segment(segmentStart.nama,segmentStart.X_awal,segmentStart.Y_awal,
          segmentStart.X_akhir,segmentStart.Y_akhir,segmentStart.bisector,
          segmentStart.tipe,segmentStart.usage)

        var regNext1 = Region("R"+regionCount, segPilihan.X_awal, segPilihan.Y_awal, segPilihan.X_akhir, segPilihan.Y_akhir, segPilihan.tipe, 0)
        regList += regNext1

        if(segPilihan.tipe == "BND"){
          val getSameSeg = segListBuffer.filter(x=> x.X_awal == segmentStart.X_akhir && x.Y_awal == segmentStart.Y_akhir && x.X_akhir == segmentStart.X_awal && x.Y_akhir == segmentStart.Y_awal)
          val segBoundSama = Segment(getSameSeg(0).nama,getSameSeg(0).X_awal,getSameSeg(0).Y_awal,
            getSameSeg(0).X_akhir,getSameSeg(0).Y_akhir,getSameSeg(0).bisector,
            getSameSeg(0).tipe,getSameSeg(0).usage)

          val indexSeg1 = segListBuffer.indexOf(segPilihan)
          val indexSeg2 = segListBuffer.indexOf(segBoundSama)
//
//          println(indexSeg1+"<- indexSeg1 ada indexSeg2 ->"+indexSeg2)

          segListBuffer(indexSeg1).usage = 1
          segListBuffer(indexSeg2).usage = 1
//          println("Hasil setelah diubah BND : "+segListBuffer(indexSeg1))
//          println("Hasil setelah diubah BND : "+segListBuffer(indexSeg2))

        } else if (segPilihan.tipe == "BIS"){
          val indexSeg = segListBuffer.indexOf(segPilihan)
          segListBuffer(indexSeg).usage=1
//          println(indexSeg+"<--- indexnya : Hasil setelah diubah : "+segListBuffer(indexSeg))

        }
//        println("X BEGIN -- dan --- X AKHIR -->" + X_begin+" --- "+segmentStart.X_akhir)
//        println("Y BEGIN -- dan --- Y AKHIR -->" + Y_begin+" --- "+segmentStart.Y_akhir)


      }

      val segmentValidate = segListBuffer.filter(x=> x.usage < 1).filter(x=>x.tipe == "BIS").take(1)
      if(segmentValidate.isEmpty){
        isSisaSegment = true
      }
//      println("END -- OF -- R"+regionCount)
//      println("===============================================================================================")
      regionCount+=1
    }
    val duration = (System.nanoTime - t1) / 1e9d
    println("durasi region contructing : "+duration+" banyak region : "+regionCount)
    println("===========================================================================================")
    return regList.toList
  }


  def labeling(regList : List[Region], segList : List[Segment], bisList : List[Bisector], pointList : List[Point]): List[Labelling2] ={
    //, bisList : List[Bisector]
    val t1 = System.nanoTime
    println("Start Labelling...")

    val labelListBuffer = new ListBuffer[Labelling2]()
    var regListBuffer = regList.to[ListBuffer]
    val processingRegion = new ListBuffer[Region]()
    val beginRegion = new ListBuffer[Region]()
    val regionName = new ListBuffer[String]()

    import spark.sqlContext.implicits._
    //TABEL REGION
    val regionDF = regListBuffer.toDF("nama","X_awal","Y_awal","X_akhir","Y_akhir","tipe", "usage")
    val tempTable = regionDF.createOrReplaceTempView("region")

    //TABEL SEGMENT
    val segmentDF = segList.toDF("nama","X_awal","Y_awal","X_akhir","Y_akhir","bisector","tipe", "usage")
    val tempTableSegment = segmentDF.createOrReplaceTempView("segment")

    //TABEL BISECTOR
    val bisectorDF = bisList.toDF("nama","tipe","m_value","b_value","point1","point2")
    val tempTableBisector = bisectorDF.createOrReplaceTempView("bisector")


    //CARI LABEL SALAH SATU REGION DENGAN CENTROID
    val preusingSQL = sqlContext.sql("select * from region where nama = 'R1'")
    val dataRegionAwal = preusingSQL.coalesce(1).collect()

    for(awalRegion <- dataRegionAwal){
      var regionya = Region(awalRegion.getString(0), awalRegion.getDecimal(1), awalRegion.getDecimal(2), awalRegion.getDecimal(3), awalRegion.getDecimal(4), awalRegion.getString(5), awalRegion.getInt(6))
      var indexReg = regListBuffer.indexOf(regionya)
      if(regionya.tipe.equals("BIS")){
        regionya.usage = 0
      } else if(regionya.tipe.equals("BND")){
        regionya.usage = 1
      }
      beginRegion += regionya
    }

    val firstCentroid = calculateCentroid(beginRegion.toList)
    val labelStart = getFirstLabel(firstCentroid, pointList)
    var current_LabelString = labelStart.label
    labelListBuffer += labelStart
    println("Processing "+labelStart.nama)

    for(bg <- beginRegion){
      if(bg.tipe.equals("BIS")){
        processingRegion += bg
      }
    }


    //------------------------------------------------------------------------
    var isSisaLabel = false
    while(isSisaLabel == false){
//      var labelDF = labelListBuffer.toDF("nama","label")
//      var tempTableLabel = labelDF.createOrReplaceTempView("labelling")

      for(prcSeg <- processingRegion){
        var labelDF = labelListBuffer.toDF("nama","label")
        var tempTableLabel = labelDF.createOrReplaceTempView("labelling")

        //CARI LABEL DARI SEGMENT INI
        val getLabelQuery = sqlContext.sql("select * from labelling where nama='"+prcSeg.nama+"'").coalesce(1).collect()

        import scala.collection.JavaConverters._
        val getPrevLabel = getLabelQuery(0).getList[String](1).asScala.toList
        //        val getPrevLabel = List("Point1","Point2","Point3","Point4")
        //ubah status segment regionya
        val idx1 = regListBuffer.indexOf(prcSeg)
        regListBuffer(idx1).usage = 1


        //DARI SEGMENT YANG DIAMBIL, DIDAPATLAH SALAH 1 SEGMENT (KEBALIKAN SEGMENT YANG DIATAS) DARI REGION BERIKUTNYA
        val nextSegmentRAW= sqlContext.sql("select * from region where X_akhir = "+prcSeg.X_awal+" and Y_akhir = "
          +prcSeg.Y_awal+" and X_awal = "+prcSeg.X_akhir+" and Y_awal = "+prcSeg.Y_akhir).coalesce(1).collect()
        val nextRegion = Region(nextSegmentRAW(0).getString(0), nextSegmentRAW(0).getDecimal(1),nextSegmentRAW(0).getDecimal(2),nextSegmentRAW(0).getDecimal(3),
          nextSegmentRAW(0).getDecimal(4), nextSegmentRAW(0).getString(5), nextSegmentRAW(0).getInt(6))
        val nextRegionName = nextRegion.nama
        //ubah status segment regionya berikutnya
        val idx2 = regListBuffer.indexOf(nextRegion)
        regListBuffer(idx2).usage = 1


        val getInfoLabelnya = sqlContext.sql("select nama from labelling where nama = '"+nextRegionName+"'")

        val checkRegion = getInfoLabelnya.coalesce(1).collect().isEmpty

        if(checkRegion){
          if(regionName.contains(nextRegionName) == false){
            regionName += nextRegionName
          }
          //SEKARANG CEK DIA ITU BISECTOR APA
          val getbisectorInfo = sqlContext.sql("select bisector from segment where X_akhir = "+prcSeg.X_awal+" and Y_akhir = "
            +prcSeg.Y_awal+" and X_awal = "+prcSeg.X_akhir+" and Y_awal = "+prcSeg.Y_akhir).coalesce(1).collect()
          val nextBisectorName = getbisectorInfo(0).getString(0)


          //QUERY KE TABEL BISECTOR BWT DAPET INFO POINTNYA
          val getPointInfo = sqlContext.sql("select point1, point2 from bisector where nama='"+nextBisectorName+"'").coalesce(1).collect()
          val thePoint1 = getPointInfo(0).getString(0)
          val thePoint2 = getPointInfo(0).getString(1)

          val labelbaru = swapLabel(getPrevLabel, thePoint1, thePoint2)
          val labelling2baru = Labelling2(nextRegionName, labelbaru)

          if(labelListBuffer.contains(labelling2baru) == false){
            labelListBuffer += labelling2baru
            println("Processing "+labelling2baru.nama)
          }
        }
      }
      processingRegion.clear()

      import spark.sqlContext.implicits._
      //TABEL REGION
      val regionDF = regListBuffer.toDF("nama","X_awal","Y_awal","X_akhir","Y_akhir","tipe", "usage")
      val tempTable = regionDF.createOrReplaceTempView("region")

      for(rg <- regionName){
        val getCandRegion = sqlContext.sql("select * from region where nama='"+rg+"' and tipe = 'BIS' and usage = 0").coalesce(1)
        for(anyReg <- getCandRegion.collect()){
          val candReg = Region(anyReg.getString(0), anyReg.getDecimal(1),anyReg.getDecimal(2),anyReg.getDecimal(3),
            anyReg.getDecimal(4), anyReg.getString(5), anyReg.getInt(6))
          processingRegion += candReg
        }
      }
      regionName.clear()

      val labelValidate = sqlContext.sql("select * from region where tipe = 'BIS' and usage = 0").collect()
      if(labelValidate.isEmpty){
        isSisaLabel = true
      }

    }

    val duration = (System.nanoTime - t1) / 1e9d
    println("durasi labelling : "+duration)
    println("===========================================================================================")

    labelListBuffer.toList
  }

  def calculateCentroid(regList : List[Region]): Point ={
    var centroidX = BigDecimal(0.0)
    var centroidY = BigDecimal(0.0)
    import scala.collection.JavaConversions._
    for (knot <- regList) {
      centroidX = centroidX+ (knot.X_awal.toDouble)
      centroidY = centroidY+ (knot.Y_awal.toDouble)
    }
    val cenX = centroidX/(regList.size.toDouble)
    val cenY = centroidY/(regList.size.toDouble)
    Point(regList(0).nama,cenX, cenY)
  }

  // SEMENTARA DI KOMEN
  def getFirstLabel(centroid : Point, pointList : List[Point]) : Labelling2 = {
    var distanceListBuffer = new ListBuffer[(String, Double)]()
    var labelComplete = new ListBuffer[String]()
    for(p <- pointList){
      val distance = Math.sqrt(Math.pow((centroid.kordinatX - p.kordinatX).toDouble, 2) + Math.pow((centroid.kordinatY - p.kordinatY).toDouble, 2))
      val tupleDis = (p.nama, distance)
      distanceListBuffer += tupleDis
    }

    val sortedTuple = distanceListBuffer.sortBy(_._2)

    for(s <- sortedTuple){
      labelComplete += s._1
    }

    Labelling2(centroid.nama, labelComplete.toList)
  }

  def swapLabel(labelList : List[String], p1: String, p2: String): List[String] ={
    val indexP1 = labelList.indexOf(p1)
    val indexP2 = labelList.indexOf(p2)

    val bufferLabel = labelList.to[ListBuffer]

    bufferLabel(indexP1) = p2
    bufferLabel(indexP2) = p1

    bufferLabel.toList
  }



}

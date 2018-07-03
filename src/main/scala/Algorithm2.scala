import scala.collection.mutable.ListBuffer

class Algorithm2 {
  var x_min, x_max, y_min, y_max = BigDecimal(123456.789)
  var adder10 = BigDecimal(5)
  val timesby = 1

  // semua val
  val bisBuffer = new ListBuffer[xBisector]()
  val vertexBuffer = new ListBuffer[xVertex]()



  var number_of_cpus =32 //default 32

  val bufferTime = new ListBuffer[(String, Double)]()

  def assignMinMax(pointList : List[Point]) ={
    val sortedpointX = pointList.sortWith(_.kordinatX > _.kordinatX)
    val sortedpointY = pointList.sortWith(_.kordinatY > _.kordinatY)
    println("minX dan minY real : "+sortedpointX.last.kordinatX+" "+sortedpointY.last.kordinatY)


    x_max = (sortedpointX.head.kordinatX+adder10)
    x_min = (sortedpointX.last.kordinatX-adder10)
    y_max = (sortedpointY.head.kordinatY+adder10)
    y_min = (sortedpointY.last.kordinatY-adder10)
    println("minX dan minY : "+x_min+" "+y_min)
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

  def getBisector(nm : String, titikPertama : Point, titikKedua: Point): xBisector ={
    var bisectornya = xBisector(nm,"BIS",BigDecimal(1), BigDecimal(1), new ListBuffer[xVertex], new ListBuffer[xSegment], titikPertama.nama, titikKedua.nama)
    if(titikPertama.kordinatX.equals(titikKedua.kordinatX)){
      //Calculate Mid Point
      val midPointX = (titikKedua.kordinatX+titikPertama.kordinatX)/2
      val midPointY = (titikKedua.kordinatY+titikPertama.kordinatY)/2

      //pake y-nya biar y=...
//      var bisectornya = Bisector(nm,"BIS", 0, midPointY, titikPertama.nama, titikKedua.nama)
      bisectornya.m_value = 0
      bisectornya.b_value = midPointY

    }else if(titikPertama.kordinatY.equals(titikKedua.kordinatY)){
      //Calculate Mid Point
      val midPointX = (titikKedua.kordinatX+titikPertama.kordinatX)/2
      val midPointY = (titikKedua.kordinatY+titikPertama.kordinatY)/2

      //pake y-nya biar y=...
//      var bisectornya = Bisector("Bis_"+(i+1),"BIS", 1, midPointX, titikPertama.nama, titikKedua.nama)
      bisectornya.m_value = 1
      bisectornya.b_value = midPointX

    }else {

      //Calculate Mid Point
      val midPointX = (titikKedua.kordinatX+titikPertama.kordinatX)/2
      val midPointY = (titikKedua.kordinatY+titikPertama.kordinatY)/2
//      val midPoint = Point("MidPoint"+titikPertama.nama+titikKedua.nama, midPointX,midPointY)
      //Find Slope m1
      val m1 = (titikKedua.kordinatY - titikPertama.kordinatY)/(titikKedua.kordinatX-titikPertama.kordinatX)

      //Find Slope m2
      val m2 = -1/m1
      //Find b on y=mx+b
      val bVal = (midPointY - (m2*midPointX))

//      var bisectornya = Bisector("Bis_"+(i+1),"BIS", m2, bVal, titikPertama.nama, titikKedua.nama)
      bisectornya.m_value = m2
      bisectornya.b_value = bVal

    }
    bisectornya
  }

  def bisector(pointList : List[Point]){
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

    for (i <- 0 to (banyakPerpen.toInt)-1) {
      if (pointer2 == pointList.length) {
        pointer1 += 1
        pointer2 = pointer1 + 1
      }

      //Get 2 Point
      val titikPertama = pointList(pointer1)
      val titikKedua = pointList(pointer2)
      val nmBis = "Bis_"+(i+1)

      val bis = getBisector(nmBis, titikPertama, titikKedua)
      bisBuffer += bis
      pointer2 += 1

    }

    //Add Boundary Line as Bisector
    //Add the x = ...
    var bisBoundX1 = xBisector("Bis_"+(banyakPerpen+1),"BND", 1, x_min, new ListBuffer[xVertex], new ListBuffer[xSegment], "-", "-")
    var bisBoundX2 = xBisector("Bis_"+(banyakPerpen+2),"BND", 1, x_max, new ListBuffer[xVertex], new ListBuffer[xSegment], "-", "-")
    //Add the y = ...
    var bisBoundY1 = xBisector("Bis_"+(banyakPerpen+3),"BND", 0, y_min, new ListBuffer[xVertex], new ListBuffer[xSegment], "-", "-")
    var bisBoundY2 = xBisector("Bis_"+(banyakPerpen+4),"BND", 0, y_max, new ListBuffer[xVertex], new ListBuffer[xSegment], "-", "-")

    //Add to List
    bisBuffer += bisBoundX1
    bisBuffer += bisBoundX2
    bisBuffer += bisBoundY1
    bisBuffer += bisBoundY2

    val duration = (System.nanoTime - t1) / 1e9d
    val tupledur = ("Bisector", duration)
    bufferTime += tupledur
    println("Durasi Perp Bisector : "+duration + " -Banyak : "+(bisBuffer.size-1))
    println("===========================================================================================")
  }

  def getVertex(nm: String, bisectorPertama: xBisector, bisectorKedua: xBisector): xVertex ={
    var bisL = new ListBuffer[xBisector]
    bisL += bisectorPertama
    bisL += bisectorKedua
    var ver = xVertex(nm, BigDecimal(1), BigDecimal(1), bisL)

    if(bisectorPertama.tipe.equals("BND") && bisectorKedua.tipe.equals("BND")){
      if(bisectorPertama.m_value.equals(0) && bisectorKedua.m_value.equals(1)){ //Berarti garis y=..
        ver.kordinatX = bisectorKedua.b_value
        ver.kordinatY = bisectorPertama.b_value
      } else if(bisectorPertama.m_value.equals(1) && bisectorKedua.m_value.equals(0)){ //Berarti garis x=..
        ver.kordinatX = bisectorPertama.b_value
        ver.kordinatY = bisectorKedua.b_value
      }

      //IF BIS INTERSECT WITH BIS
    } else if(bisectorPertama.tipe.equals("BIS") && bisectorKedua.tipe.equals("BIS")){

      //IF BIS INTERSECT WITH BIS THAT HAVE MVAL 0/1
      if(bisectorPertama.m_value.equals(0)) {
        //berarti bisector pertama y=...

        if(bisectorKedua.m_value.equals(1)){
          if(bisectorKedua.b_value <= x_max && bisectorKedua.b_value >= x_min) {
            if (bisectorPertama.b_value <= y_max && bisectorPertama.b_value >= y_min) {
              ver.kordinatX = bisectorKedua.b_value
              ver.kordinatY = bisectorPertama.b_value
            }
          }
        } else if(!bisectorKedua.m_value.equals(0) && !bisectorKedua.m_value.equals(1)){

          var vertexX = (bisectorPertama.b_value - bisectorKedua.b_value) / bisectorKedua.m_value

          if(vertexX <= x_max && vertexX >= x_min) {
            if (bisectorPertama.b_value <= y_max && bisectorPertama.b_value >= y_min) {
              ver.kordinatX = vertexX
              ver.kordinatY = bisectorPertama.b_value
            }
          }
        }

      } else if(bisectorKedua.m_value.equals(0)){
        //berarti bisector kedua y=...

        if(bisectorPertama.m_value.equals(1)){
          if(bisectorPertama.b_value <= x_max && bisectorPertama.b_value >= x_min) {
            if (bisectorKedua.b_value <= y_max && bisectorKedua.b_value >= y_min) {
              ver.kordinatX = bisectorPertama.b_value
              ver.kordinatY = bisectorKedua.b_value
            }
          }
        } else if(!bisectorPertama.m_value.equals(0) && !bisectorPertama.m_value.equals(1)){
          var vertexX = (bisectorKedua.b_value - bisectorPertama.b_value) / bisectorPertama.m_value

          if(vertexX <= x_max && vertexX >= x_min) {
            if (bisectorKedua.b_value <= y_max && bisectorKedua.b_value >= y_min) {
              ver.kordinatX = vertexX
              ver.kordinatY = bisectorKedua.b_value
            }
          }
        }


      }else if(bisectorPertama.m_value.equals(1)){
        //berarti bisector pertama x=...

        if(bisectorKedua.m_value.equals(0)){
          if(bisectorPertama.b_value <= x_max && bisectorPertama.b_value >= x_min) {
            if (bisectorKedua.b_value <= y_max && bisectorKedua.b_value >= y_min) {
              ver.kordinatX = bisectorPertama.b_value
              ver.kordinatY = bisectorKedua.b_value
            }
          }

        } else if(!bisectorKedua.m_value.equals(0) && !bisectorKedua.m_value.equals(1)){
          var vertexY = (bisectorPertama.b_value*bisectorKedua.m_value)+bisectorKedua.b_value

          if(bisectorPertama.b_value <= x_max && bisectorPertama.b_value >= x_min) {
            if (vertexY <= y_max && vertexY >= y_min) {
              ver.kordinatX = bisectorPertama.b_value
              ver.kordinatY = vertexY
            }
          }
        }

      } else if(bisectorKedua.m_value.equals(1)){
        //berarti bisector kedua x=...

        if(bisectorPertama.m_value.equals(0)){
          if(bisectorKedua.b_value <= x_max && bisectorKedua.b_value >= x_min) {
            if (bisectorPertama.b_value <= y_max && bisectorPertama.b_value >= y_min) {
              ver.kordinatX = bisectorKedua.b_value
              ver.kordinatY = bisectorPertama.b_value
            }
          }


        } else if(!bisectorPertama.m_value.equals(0) && !bisectorPertama.m_value.equals(1)){
          var vertexY = (bisectorKedua.b_value*bisectorPertama.m_value)+bisectorPertama.b_value

          if(bisectorKedua.b_value <= x_max && bisectorKedua.b_value >= x_min) {
            if (vertexY <= y_max && vertexY >= y_min) {
              ver.kordinatX = bisectorKedua.b_value
              ver.kordinatY = vertexY
            }
          }
        }


      } else {
        //Calculate intersection X,Y
        val intersect_x = ((bisectorKedua.b_value-bisectorPertama.b_value) / (bisectorPertama.m_value-bisectorKedua.m_value))
        val intersect_y = (bisectorKedua.m_value*intersect_x)+bisectorKedua.b_value

        if(intersect_x <= x_max && intersect_x >= x_min){
          if(intersect_y <= y_max && intersect_y >= y_min){
            ver.kordinatX = intersect_x
            ver.kordinatY = intersect_y
          }
        }
      }


    } else {

      //IF BIS INTERSECT WITH BOUND
      if(bisectorPertama.tipe.equals("BND")){
        if(bisectorPertama.m_value.equals(0)){ //Y=...
          if(bisectorKedua.m_value.equals(1)){
            if(bisectorKedua.b_value <= x_max && bisectorKedua.b_value >= x_min){
              if(bisectorPertama.b_value <= y_max && bisectorPertama.b_value >= y_min){
                ver.kordinatX = bisectorKedua.b_value
                ver.kordinatY = bisectorPertama.b_value
              }
            }

          }else if(!bisectorKedua.m_value.equals(0) && !bisectorKedua.m_value.equals(1)){
            var vertexX = (bisectorPertama.b_value-bisectorKedua.b_value)/bisectorKedua.m_value
            if(vertexX <= x_max && vertexX >= x_min){
              if(bisectorPertama.b_value <= y_max && bisectorPertama.b_value >= y_min){
                ver.kordinatX = vertexX
                ver.kordinatY = bisectorPertama.b_value
              }
            }
          }




        } else if (bisectorPertama.m_value.equals(1)) { //X = ...
          if(bisectorKedua.m_value.equals(0)) {
            if (bisectorPertama.b_value <= x_max && bisectorPertama.b_value >= x_min) {
              if (bisectorKedua.b_value <= y_max && bisectorKedua.b_value >= y_min) {
                ver.kordinatX = bisectorPertama.b_value
                ver.kordinatY = bisectorKedua.b_value
              }
            }
          }else if(!bisectorKedua.m_value.equals(0) && !bisectorKedua.m_value.equals(1)){
            var vertexY = (bisectorPertama.b_value*bisectorKedua.m_value)+bisectorKedua.b_value
            if(bisectorPertama.b_value <= x_max && bisectorPertama.b_value >= x_min){
              if(vertexY <= y_max && vertexY >= y_min){
                ver.kordinatX= bisectorPertama.b_value
                ver.kordinatY= vertexY
              }
            }
          }
        }

      } else if(bisectorKedua.tipe.equals("BND")){
        if(bisectorKedua.m_value.equals(0)){ //Y=...
          if(bisectorPertama.m_value.equals(1)){
            if (bisectorPertama.b_value <= x_max && bisectorPertama.b_value >= x_min) {
              if (bisectorKedua.b_value <= y_max && bisectorKedua.b_value >= y_min) {
                ver.kordinatX= bisectorPertama.b_value
                ver.kordinatY= bisectorKedua.b_value
              }
            }

          } else if(!bisectorPertama.m_value.equals(0) && !bisectorPertama.m_value.equals(1)){
            var vertexX = (bisectorKedua.b_value-bisectorPertama.b_value)/bisectorPertama.m_value
            if(vertexX <= x_max && vertexX >= x_min){
              if(bisectorKedua.b_value <= y_max && bisectorKedua.b_value >= y_min){
                ver.kordinatX= vertexX
                ver.kordinatY= bisectorKedua.b_value
              }
            }
          }

        } else if(bisectorKedua.m_value.equals(1)) { //X = ...
          if(bisectorPertama.m_value.equals(0)) {
            if (bisectorKedua.b_value <= x_max && bisectorKedua.b_value >= x_min) {
              if (bisectorPertama.b_value <= y_max && bisectorPertama.b_value >= y_min) {
                ver.kordinatX= bisectorKedua.b_value
                ver.kordinatY= bisectorPertama.b_value
              }
            }

          } else if(!bisectorPertama.m_value.equals(0) && !bisectorPertama.m_value.equals(1)){
            var vertexY = (bisectorKedua.b_value*bisectorPertama.m_value)+bisectorPertama.b_value
            if(bisectorKedua.b_value <= x_max && bisectorKedua.b_value >= x_min){
              if(vertexY <= y_max && vertexY >= y_min){
                ver.kordinatX= bisectorKedua.b_value
                ver.kordinatY= vertexY
              }
            }
          }
        }

      }
    }
    return ver
  }

  def mergeVertex(oldv: xVertex, newv: xVertex){
    for(xxbis <- newv.bis){
      val iscontain = oldv.bis.contains(xxbis)
      if(iscontain == false){
        oldv.bis += xxbis

        val idx = bisBuffer.indexOf(xxbis)
        bisBuffer(idx).vertex += oldv

      }
    }

  }

  def vertex(){
    val t1 = System.nanoTime
    var first = true
    var countV = 1
    var nmV = "v_"+countV

    for(i <- 0 to (bisBuffer.size-1)){
      for(j<- (i+1) to (bisBuffer.size-1)){
        val vertex = getVertex(nmV, bisBuffer(i), bisBuffer(j))
        println("first vertex : "+vertex.kordinatX+","+vertex.kordinatY)
        if(first) {
          vertexBuffer += vertex
          bisBuffer(i).vertex += vertex
          bisBuffer(j).vertex += vertex

          countV += 1
          nmV = "v_" + countV
          first = false
        }
//        } else {
//          for (vv <- vertexBuffer){
//            if(vv.kordinatX.equals(vertex.kordinatX) && vv.kordinatY.equals(vertex.kordinatY)){
//              mergeVertex(vv,vertex)
//            } else {
//              vertexBuffer += vertex
//              bisBuffer(i).vertex += vertex
//              bisBuffer(j).vertex += vertex
//
//              countV += 1
//              nmV = "v_"+countV
//            }
//
//          }
//
//        }


      }
    }
  }







}

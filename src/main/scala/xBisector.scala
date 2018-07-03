import scala.collection.mutable.ListBuffer

case class xBisector(
                      nama: String,tipe: String,
                      var m_value: BigDecimal,
                      var b_value: BigDecimal,
                      var vertex: ListBuffer[xVertex],
                      var segment: ListBuffer[xSegment],
                      point1: String,
                      point2: String)
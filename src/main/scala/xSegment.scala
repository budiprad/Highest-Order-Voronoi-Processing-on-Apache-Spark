case class xSegment(
                     var nama: String,
                     var start: xVertex,
                     var end: xVertex,
                     var bisector: xBisector,
                     var next: List[xSegment],
                     var usage:Int)
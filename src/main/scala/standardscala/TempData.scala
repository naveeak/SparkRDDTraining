package standardccala

case class TempData(day:Int,doy:Int,month:Int,year:Int,
    precip:Double,snow:Double,tave:Double,tmax:Double,tmin:Double )
    
object TempData {
  def main(args:Array[String])={
    val source=scala.io.Source.fromFile("MN212142_9392.csv")
    val lines=source.getLines().drop(1)
    val data=lines.map{line => 
      val p =line.split(",")
      TempData(p(0).toInt,p(1).toInt,p(2).toInt,p(4).toInt,p(5).toDouble
          ,p(6).toDouble,p(7).toDouble,p(8).toDouble,p(9).toDouble)
    }.toArray
    data take(5) foreach println  
    source.close()
    
  }
}
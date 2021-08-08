package standardscala

import scala.io.Source
  
case class TempData(day:Int,doy:Int,month:Int,year:Int,
    precip:Double,snow:Double,tave:Double,tmax:Double,tmin:Double )
    
object TempData {
  def toDoubleOrNeg(s:String):Double={
    try{
       s.toDouble
    }catch{
      case _: NumberFormatException => -1
    }
   
     
  }
  def main(args:Array[String])={   
    val source=scala.io.Source.fromFile("MN212142_9392.csv")
    val lines=source.getLines().drop(1)
    val data=lines.flatMap{ line => 
      val p =line.split(",")
      if(p(7)=="."||p(8)=="."||p(9)==".") Seq.empty else
      Seq(TempData(p(0).toInt,p(1).toInt,p(2).toInt,p(4).toInt,toDoubleOrNeg(p(5))
          ,toDoubleOrNeg(p(6)),p(7).toDouble,p(8).toDouble,p(9).toDouble))
    }.toArray
    source.close()
    
    val maxTemp=data.map(_.tmax).max
    val HotDays=data.filter(d => maxTemp == d.tmax)
    println(s"1==>${HotDays.mkString(", ")}")
    
    val Hotdays2=data.reduce((d1,d2) => if(d1.tmax>=d2.tmax) d1 else d2)
    println(s"2==>${Hotdays2}")
    
    val Hotdays3=data.maxBy(_.tmax)
    println(s"3==>${Hotdays3}")
    //https://www.youtube.com/watch?v=juIWMF3-sbk&list=PLLMXbkbDbVt-f6qwCZqfq7e_6eT8aFxzT&index=5
    val rainyCount=data.count(_.precip>=1.0)//instead of doing like creating a filter and couting to avoid a extra collection reduction
    println(rainyCount+s" Percent ${rainyCount*100.0/data.length}")
    
    val (totalTempRainyDay,rainyDayCount)= data.foldLeft(0.0->0){ case ((sum,count),d) =>
    if(d.precip>=1.0) (sum,count) else (sum+d.tmax,count+1)    
    }
    println(s"avg temp on rainy day ${totalTempRainyDay/rainyDayCount}")  
    
    
  }
}
package sparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import standardscala.TempData
  

object RDDTempData {
  def main(args:Array[String]):Unit={
    val SparkConf=new SparkConf().setAppName("Test").setMaster("local[*]")
    val sc = new SparkContext(SparkConf)
    val lines=sc.textFile("MN212142_9392.csv").filter(!_.contains("Day"))
    val data= lines.flatMap(line=>{
      val p=line.split(",")
      if(p(7)=="."||p(8)=="."||p(9)==".") Seq.empty 
      else Seq(TempData(p(0).toInt,p(1).toInt,p(2).toInt,p(4).toInt,TempData.toDoubleOrNeg(p(5))
          ,TempData.toDoubleOrNeg(p(6)),p(7).toDouble,p(8).toDouble,p(9).toDouble))
      })
      //data.take(5) foreach println
      data.cache()
      println(data.max()(Ordering.by(_.tmax)))
      println(data.reduce((td1,td2)=>if(td1.tmax >td2.tmax) td1 else td2))
      
      val hotdayTemp=data.map(_.tmax).max()
      val hotDays=data.filter(_.tmax==hotdayTemp)
      println(s"Hotdays ares ${hotDays.collect().mkString(", ")} ")
      
      val (rainyDays,totalCount)=data.aggregate((0,0))(
          {case(t,d) => {
            if(d.precip>=1.0) (t._1+1,t._2+1) else (t._1,t._2+1)
          }},{case(a:(Int,Int),b:(Int,Int))=>(a._1+b._1,a._2+b._2)}   
      )
      println(s"rainydays $rainyDays $totalCount pecentage ${rainyDays*100.0/totalCount}")
      
      val (rainyDays1,totalCount1)=data.aggregate((0,0))(
          {case((r1,c1),d) => {
            if(d.precip>=1.0) (r1+1,c1+1) else (r1,c1+1)
          }},{case(a:(Int,Int),b:(Int,Int))=>(a._1+b._1,a._2+b._2)}   
      )
      
  }
}
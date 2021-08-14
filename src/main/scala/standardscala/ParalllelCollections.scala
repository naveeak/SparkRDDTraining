package standardscala



object ParalllelCollections extends App{
   def toDoubleOrNeg(s:String):Double={
    try{
       s.toDouble
    }catch{
      case _: NumberFormatException => -1
    }
  }
   val source=scala.io.Source.fromFile("MN212142_9392.csv")
    val lines=source.getLines().drop(1)
    val data=lines.flatMap{ line => 
      val p =line.split(",")
      if(p(7)=="."||p(8)=="."||p(9)==".") Seq.empty else
      Seq(TempData(p(0).toInt,p(1).toInt,p(2).toInt,p(4).toInt,toDoubleOrNeg(p(5))
          ,toDoubleOrNeg(p(6)),p(7).toDouble,p(8).toDouble,p(9).toDouble))
    }.toArray
    source.close()
    //in parallel collection the function must be associtive ie addition otherwise the result would be diff ...
    val (rainyTemp,rainycount)= data.foldLeft(0.0->0){ case((sum,count),d) =>(
    if (d.precip<1.0) (sum,count) else (sum+d.tmax,count+1))
    }
    
    println(s"avg temp on rainy day ${rainyTemp/rainycount}")  
    //parallel function
     val (rainyTemp1,rainycount1)= data.par.foldLeft(0.0->0){ case((sum,count),d) =>(
    if (d.precip<1.0) (sum,count) else (sum+d.tmax,count+1))
    }
    println(s"avg temp on rainy day1 ${rainyTemp1/rainycount1}")  
    
//     val (rainyTemp2,rainycount2)= data.par.aggregate(0.0->0){ (case((sum,count),d) => (
//    if (d.precip<1.0) (sum,count) else (sum+d.tmax,count+1)),(case((s1,c1),(s2,c2)))
//    }
    val (rainyTemp2,rainycount2)=data.par.aggregate(0.0->0)(
        {
          case((sum,count),d) => {
            if(d.precip<1.0) (sum,count)
            else (sum+d.tmax,count+1)}},
        {
          case((s1,c1),(s2,c2))=>(s1+s2,c1+c2)})
    
    println(s"avg temp on rainy day2 ${rainyTemp2/rainycount2}")  
    
    
}


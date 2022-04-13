import java.io.File
import scala.collection.mutable
import scala.io.Source

object PlainScala {
  def main(args: Array[String]): Unit = {
      val files = getFilesList("C:\\Users\\Vinodh\\Downloads\\SensorStats\\data_files\\")
    val sensorHumidityValues=new mutable.HashMap[String,List[Int]]()
    val sensonNaNHumidityValues=new mutable.HashMap[String,List[String]]()
    var naNCount = 0
    var processedCount =0
    files.foreach(file=>{
        val lines= Source.fromFile(file).getLines()
         val eachEvent= lines.map(line=>{
            val payload= line.split(",")
            (payload(0),payload(1))
          }).filter(x=>x._1!="sensor-id")

        eachEvent.foreach(x=>{
          processedCount=processedCount+1
          if(x._2!="NaN"){
            if(sensorHumidityValues.contains(x._1)){
              var existing: List[Int]=sensorHumidityValues.get(x._1).get
              existing=existing :+ Integer.parseInt(x._2)
              sensorHumidityValues.put(x._1,existing)
            }else{
              sensorHumidityValues.put(x._1,List(Integer.parseInt(x._2)))
            }
          }else{
            naNCount=naNCount+1
            if(sensonNaNHumidityValues.contains(x._1)){
              var existing=sensonNaNHumidityValues.get(x._1).get
              existing = existing :+ x._2
              sensonNaNHumidityValues.put(x._1,existing)
            }else{
              sensonNaNHumidityValues.put(x._1,List(x._2))
            }
          }

        })

      })
   var listOfSensorStats: List[SensorStats]=List()
    /**
     * Creating map of sensorids with sorted on average humidity
     */
    var mapWithSortedOnAvg: mutable.HashMap[String,Int]=new mutable.HashMap[String,Int]()
    sensorHumidityValues.foreach(x=>{
      mapWithSortedOnAvg.put(x._1,x._2.sum/x._2.length)
     })

    /**
     * Creating sensor statistics objects based on ordered map mapWithSortedOnAvg
     */
    val orderedMap=mapWithSortedOnAvg.toList.sortBy(_._2)(Ordering[Int].reverse).toMap

    orderedMap.foreach(x=>{
     val humidValueForSenor= sensorHumidityValues.get(x._1).get
      val sensorStats=SensorStats(x._1,humidValueForSenor.min.toString,x._2.toString,humidValueForSenor.max.toString)

     listOfSensorStats=listOfSensorStats :+ sensorStats
    })

    /**
     * Appending sensor statistics with only NaN values to Original Snesor Statistics list
     */
    val sensorsWithOnlyNaNs=    (sensonNaNHumidityValues.keySet diff sensorHumidityValues.keySet).toSet
    sensorsWithOnlyNaNs.foreach(x=>{
      val sensorStats=SensorStats(x,"NaN","Nan","NaN")
     listOfSensorStats= listOfSensorStats :+ sensorStats
    })

    println("Num of processed files:\t"+files.length)
    println("Num of processed measurements:\t"+processedCount)
    println("Num of failed measurements:\t"+naNCount)
    println("Sensors with highest avg humidity:")
    println("sensor-id,min,avg,max")
    for (elem <- listOfSensorStats) {
      println(elem.sensorId+","+elem.min+","+elem.avg+","+elem.max)
    }
  }
  def getFilesList(dir: String): List[File]={
    val filesDir= new File(dir);
    filesDir.listFiles().filter(_.isFile).toList
  }
}


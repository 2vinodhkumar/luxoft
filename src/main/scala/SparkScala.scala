import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType

object SparkScala {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local[4]").appName("Summary").config("spark.io.compression.codec","snappy").getOrCreate()
    val df=spark.read.option("header",true).csv("C:\\Users\\Vinodh\\Downloads\\SensorStats\\data_files\\")
    val df1=df.filter(df("humidity")=!="NaN")
    import org.apache.spark.sql.functions._
    val df2=df1.withColumn("humidity",col("humidity").cast(IntegerType))
    val df3=df2.groupBy("sensor-id").agg(min("humidity").as("min"),avg("humidity").as("avg"),max("humidity").as("max"))
    val df4=df3.orderBy(desc("avg"))

    val df5=df.filter(df("humidity")==="NaN")
    val df6=df5.join(df3,Seq("sensor-id"),"leftanti")
    val df7=df6.withColumn("min",lit("NaN")).withColumn("avg",lit("NaN")).withColumn("max",lit("NaN")).drop("humidity")
    val statsDF=df4.union(df7)
    println("Num of processed files:\t"+df.inputFiles.length)
    println("Num of processed measurements:\t"+df.count())
    println("Num of failed measurements:\t"+df5.count())
    println("Sensors with highest avg humidity:")
    statsDF.show()
  }
}

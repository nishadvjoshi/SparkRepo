package org.nvj


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._



object SparkSQL {
  
  case class City (ID:String, Name:String, CountryCode:String, District:String, Population:String)
  
    def cityMapper(line:String): City = {
    
    val data = line.filter(_ != "ID")
    val fields = data.split(';')
    //val filtered = fields.filter(_ != "ID")
  
    val city: City = City (fields(0), fields(1), fields(2), fields(3), fields(4))
    return city
  
    }
  
  def main(args: Array[String] ){
    
    Logger.getLogger("org.nvj").setLevel(Level.ERROR)
    
      val sc = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
     val cityFile = sc.sparkContext.textFile("F:/Trainings/CITI/SampleFiles/City_Delimited.txt")
    //val cityFile = sc.read.option("header", "true").csv("F:/Trainings/CITI/SampleFiles/City_Delimited.txt")
    
    
    val cityTB = cityFile.map(cityMapper)
    import sc.implicits._
    val cityTable = cityTB.toDS()
    
    cityTable.printSchema()
    cityTable.createOrReplaceTempView("cityView")
    
    val cityRecords = sc.sql("Select * from cityView where CountryCode like 'IND' ")
    
    val results = cityRecords.collect()
    
    results.foreach(println)
    
    sc.stop()

    
  
    
  }
  
}
package org.nvj

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._


object SparkSQLMultipleFiles {
  
  case class nyseDividend (exchange:String, stock_symbol:String, date:String, dividends:String)
  
  def dividenmapper (line:String): nyseDividend = {
    
      val data = line.filter(_ != "exchange")
      val fields = data.split(',')
      
      val NYSEDiv: nyseDividend = nyseDividend (fields(0), fields(1), fields(2), fields(3))
      return NYSEDiv
    
    
  }
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org.nvj").setLevel(Level.ERROR)

    val sc = SparkSession
     .builder
     .appName("SparkSQLMultipleFiles")
     .master("local[*]")
     .config("spark.sql.warehouse.dir", "file:///C:/temp")
     .getOrCreate()
     
      //Here We are using wholeTextFiles to point the folder which contains list of files
      //Following statement will create a tuple with [FileName, ContentFromFile]. It will, therefore will have 26 records in total 
      val nyseDivFile = sc.sparkContext.wholeTextFiles("F:/Trainings/CITI/Assignments/Day2/Assignment2/DividendFiles/*.csv", 4)
      
      
      //Once the tuple [FileName, ContentFromFile] is created, we need to extract only content from all 26 files
      //Following statement achieves that 
      val nyseRecord = nyseDivFile.flatMap(etup => etup._2.split("\\n"))
     
      
      val nyseTAB = nyseRecord.map(dividenmapper)      
      
      import sc.implicits._
      val nyseTable = nyseTAB.toDS()
    
      nyseTable.printSchema()
      nyseTable.createOrReplaceTempView("nyseDIVView")
    
      val nyseRecords = sc.sql("Select MAX(dividends) from nyseDIVView GROUP BY stock_symbol")
    
      val results = nyseRecords.collect()
    
      results.foreach(println)
    
      sc.stop()
    
  }
  
  
  
}
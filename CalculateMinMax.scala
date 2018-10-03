package org.nvj

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max
import scala.math.min
import org.apache.spark.sql.catalyst.expressions.StartsWith


object CalculateMinMax {
  
  def parseNYSE (line: String) = {
    
    val data = line.filter(_ != "exchange")
    val fields = data.split(",")    
    val exchange = fields(0)
    val stock_symbol = fields(1)
    val date = fields(2)
    val dividends = fields(3).trim().toDouble
     
    (exchange,stock_symbol,date,dividends)

  }
  
  def main(agrs: Array[String]) {
    
    //Set log level to print only errors
    Logger.getLogger("org.nvj").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "CalculateMinMax")
    
    val nyseFile = sc.wholeTextFiles("F:/Trainings/CITI/Assignments/Day2/Assignment2/DividendFiles/*.csv", 4)
    
    val nyseRecord = nyseFile.flatMap(etup => etup._2.split("\\n"))
    
     val nyseheader = nyseRecord.filter(x => !(x.toString().contains("exchange")))
    
     val nyseData = nyseheader.map(parseNYSE)
    
     val KeyRDD = nyseData.map(x => (x._2,x._4))
    
     val MinDividend = KeyRDD.reduceByKey((x,y) => min(x,y))
    
     val MaxDividend = KeyRDD.reduceByKey((x,y) => max(x,y))
    
     val MinResult = MinDividend.collect()
     val MaxResult = MaxDividend.collect()
    
     MinResult.foreach(println)
     MaxResult.foreach(println)

     sc.stop()
    
  }
  
  
}
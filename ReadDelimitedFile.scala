package org.nvj

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Encoders



object ReadDelimitedFile {
  
  case class NYSEDaily(exchange:String, stock_symbol:String, date:String, stock_price_open:String, stock_price_high:String, stock_price_low:String , stock_price_close:String, stock_volume:String, stock_price_adj_close:String)
  

  
  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val textRDD = sc.textFile("F:\\Trainings\\CITI\\Assignments\\Day2\\Assignment2\\NYSE_daily_prices_C.csv")
    //println(textRDD.foreach(println)
    val nyseRdd = textRDD.map {
      line =>
        val col = line.split("|")
        NYSEDaily(col(0), col(1), col(2), col(3), col(4), col(5), col(6),col(7),col(8))
    }
    val empDF = nyseRdd.toDF()
    empDF.show(5)
    
    /* Spark 2.0 or up
      val nyseDF= sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("F:\\Trainings\\CITI\\Assignments\\Day2\\Assignment2\\NYSE_daily_prices_C.csv")
    */

  }
  
}
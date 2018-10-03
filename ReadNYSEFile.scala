package org.nvj


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SQLContext


object ReadNYSEFile {
  case class NYSEDaily(exchange: String, stock_symbol: String, date: String, stock_price_open: String, stock_price_high: String, stock_price_low: String, stock_price_close: String, stock_volume: String, stock_price_adj_close: String)

  def main(args: Array[String]) {

    val conf = new SparkConf()
    val sc = new SparkContext(conf.setMaster("local").setAppName("Read CSV File"))
    val files = FileSystem.get(sc.hadoopConfiguration).listStatus(new Path("F:\\Trainings\\CITI\\SampleFiles\\FileWatcher"))
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    files.foreach(filename => {
      val filePath = filename.getPath.toString()
      val textRDD = sc.textFile(filePath)
      val nyseRdd = textRDD.map {
        line =>
         val col = line.split(",")
         NYSEDaily(col(0), col(1), col(2), col(3), col(4), col(5), col(6), col(7), col(8))
      }
      
      val nyseDF = nyseRdd.toDF()
      nyseDF.registerTempTable("NYSE")
      
      val maxStock = sqlContext.sql("SELECT distinct stock_symbol FROM NYSE")
      val result = maxStock.queryExecution;
      
      maxStock.write.text("F:/SparkSQL")
    })
  
  }

}
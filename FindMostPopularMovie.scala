package org.nvj

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._



object FindMostPopularMovie {
  
  def main(args: Array[String]) {
    
    //Set Log4j to ERROR only
    Logger.getLogger("org.nvj").setLevel(Level.ERROR)
    
    //Create Spark Context using every core of the local machine
    val sc = new SparkContext("local[*]", "FindMostPopularMovie")
    
    //Read Source File
    val lines = sc.textFile("F:/Trainings/Hadoop/Scala/ml-100k/u.data")
    
    val movies = lines.map(x => (x.split("\t")(1).toInt, 1))
                 
    val moviescount = movies.reduceByKey((x,y) => x + y)
    
    val fliped = moviescount.map(x => (x._2, x._1) )
    
    val sorted = fliped.sortByKey()
    
    //val top10 = sorted.top(10)
    
    val results = sorted.collect()
    
    results.foreach(println)
    
    
 
    
  }
  
}
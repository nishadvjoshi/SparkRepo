package org.nvj

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import java.nio.charset.CodingErrorAction
import scala.io.Source
import scala.io.Codec



object PopularMoviesBroadcast {
  
  
  def loadMovieNames() : Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    
    var movieNames:Map[Int, String] = Map()
    
     val lines = Source.fromFile("F:/Trainings/Hadoop/Scala/ml-100k/u.item").getLines()
     for (line <- lines) {
       var fields = line.split('|')
       if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
       }
     }
    
     return movieNames
    
    
  }
  
  def main(args : Array[String]) {
    
    Logger.getLogger("org.nvj").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "PopularMoviesBroadcast")
    
    // Create a broadcast variable of our ID -> movie name map
    var nameDict = sc.broadcast(loadMovieNames)
    
    val lines = sc.textFile("F:/Trainings/Hadoop/Scala/ml-100k/u.data")
    
    val movies = lines.map(x => (x.split("\t")(1).toInt, 1))
    
    val moviescount = movies.reduceByKey((x,y) => x + y)
    
    val flipped = moviescount.map(x => (x._2,x._1))
    
    val sorted = flipped.sortByKey()
    
    val sortedMoviesWithNames = sorted.map(x => (nameDict.value(x._2), x._1))
    
    val results = sortedMoviesWithNames.collect()
    
    results.foreach(println)
    
    
  }
  
}
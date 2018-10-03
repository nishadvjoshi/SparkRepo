package org.nvj

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._



object JoinDataSets {
  
    case class City (ID:String, Name:String, CountryCode:String, District:String, Population:String)
  
    def cityMapper(line:String): City = {
    
    val data = line.filter(_ != "ID")
    val fields = data.split(';')
    //val filtered = fields.filter(_ != "ID")
  
    val city: City = City (fields(0), fields(1), fields(2), fields(3), fields(4))
    return city
  
    }
    
    case class Country (Code: String, CountryName: String, Continent: String, Region: String, SurfaceArea: String, IndepYear: String, Population: String, LifeExpectancy: String, GNP: String, GNPOld: String, LocalName: String, GovernmentForm: String, HeadOfState: String, Capital: String, Code2: String)
    
    def countryMapper(line: String): Country = {
      val data = line.filter(_ != "Code")
      val fields = line.split(';')
      val country: Country = Country(fields(0), fields(1), fields(2), fields(3), fields(4), fields(5), fields(6), fields(7), fields(8), fields(9), fields(10), fields(11), fields(12), fields(13), fields(14))
    
      return country
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
    val cityTB = cityFile.map(cityMapper)
    import sc.implicits._
    val cityTable = cityTB.toDS()
    
    cityTable.printSchema()
    cityTable.createOrReplaceTempView("cityView")
    
    
    
    
    val countryFile = sc.sparkContext.textFile("F:/Trainings/CITI/SampleFiles/Country_Delimited.txt")
    val countryTB = countryFile.map(countryMapper)
    import sc.implicits._
    val countryTable = countryTB.toDS()
    
    countryTable.printSchema()
    countryTable.createOrReplaceTempView("countryView")
    
    
    val cityCountryJoin = sc.sql("Select Name, CountryName from cityView INNER JOIN countryView ON cityView.CountryCode = countryView.Code")
    val results = cityCountryJoin.collect()
    
    results.foreach(println)
    
    sc.stop()
   
    
    
    
    
    
  }
  
}
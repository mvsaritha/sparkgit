package GITtrail
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object GITobj {
  def main(args:Array[String]):Unit={
    
  val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					
					val spark=SparkSession.builder().getOrCreate()	
					
					import spark.implicits._
					println("==================Reading data====================")
					
				val jsondf= spark.read.format("json").option("multiLine","true").load("file:///C://Data//Array.json")
				jsondf.show()
				jsondf.printSchema()
				
				println("==================flat data====================")
				
				val flatDF = jsondf.select("first_name",
					                            "second_name",
					                            "Students",
					                            "address.Permanent_address",
					                            "address.temporary_address")
					                            .withColumn("Students",explode(col("Students")))
					flatDF.show()
					flatDF.printSchema()
					
					
			println("==================complex data generation====================")
			
			val complexdf = flatDF.groupBy("first_name","second_name","Permanent_address","temporary_address")
			                .agg(collect_list("Students").alias("Students"))
			                
      val finalcomplexdf = complexdf.select(
                                      col("Students"),
                                      
                                      struct(
                                          col("Permanent_address"),
                                          col("temporary_address")
                                      
                                      
                                      ).alias("address"),
                                      
                                      col("first_name"),
                                      col("second_name")
      
     
      )			                
			                
			                finalcomplexdf.show()
			                finalcomplexdf.printSchema()
			                
			                
			                
					
					
					
					
					
					
  }
  
}
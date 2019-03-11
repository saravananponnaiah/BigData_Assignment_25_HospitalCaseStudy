package com.spark.hospital

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object HospitalCaseStudy {
  def main(args : Array[String]) : Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Creating a spark context object to run in local machine
    val spark = SparkSession.builder.appName("Hospital Charges Analysis").master("local").getOrCreate()
    import spark.implicits._
    
    // Create a dataframe and load the building csv file
    val dfInpatientCharges = spark.sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("E:\\Acadgild\\Data\\inpatientCharges.csv")
      
    //dfInpatientCharges.printSchema()
    dfInpatientCharges.registerTempTable("hospital_data")
    val avgCoveredChargesPerState = spark.sql("SELECT ProviderState, AVG(AverageCoveredCharges) FROM hospital_data GROUP BY ProviderState")
    val avgTotalPaymentsPerState = spark.sql("SELECT ProviderState, AVG(AverageTotalPayments) FROM hospital_data GROUP BY ProviderState")
    val avgMedicarePaymentsPerState = spark.sql("SELECT ProviderState,AVG(AverageMedicarePayments) FROM hospital_data GROUP BY ProviderState")
    val totalDischargesPerStatePerDisease = spark.sql("SELECT DRGDefinition, ProviderState, SUM(TotalDischarges) as TotalDischarges FROM hospital_data GROUP BY DRGDefinition, ProviderState ORDER BY TotalDischarges DESC")
    
    println("****** AVERAGE COVERED CHARGES PER STATE ******")
    avgCoveredChargesPerState.show()
    println("****** AVERAGE TOTAL PAYMENTS PER STATE ******")
    avgTotalPaymentsPerState.show()
    println("****** AVERAGE MEDICARE PAYMENTS PER STATE ******")
    avgMedicarePaymentsPerState.show()
    println("****** TOTAL DISCHARGES PER DISEASE PER STATE BY DESCENDING ORDER ******")
    totalDischargesPerStatePerDisease.show()
  }  
}
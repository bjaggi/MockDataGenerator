package com.rbcone.am.process

import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col



/**
  * Created by ${Brijesh_Jaggi} on 2017/12/05.
  */
object AMCDRFilesReader {
  // Create the context with a 1 second batch size
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\Softwares\\Winutils")

    val conf = new SparkConf().setAppName("LogSQL").setMaster("local[*]").set("spark.sql.crossJoin.enabled", "true")


    Logger.getLogger("org").setLevel(Level.INFO)

    val session = SparkSession.builder().
      appName("AM_CDRFiles_Processor").
      config("spark.sql.crossJoin.enabled","true").
      master("local[*]").
      getOrCreate()

    val dataFrameReader = session.read

    //val makerSpace = session.read.option("header", "true").csv("in/uk-makerspaces-identifiable-data.csv")
    val clientFile = dataFrameReader.json("input_cdr_files/CDR_INTSClient_20170101000000_0002.json")
    val relationShipFile = dataFrameReader.json("input_cdr_files/CDR_INTSRelationship_20170101000000_0002.json")
    val clientFileWithSelectedColumns = clientFile.select("CLIENT_ID", "ISO_LEGAL_NAME",  "ENTITY_TYPE","CLIENT_TYPE_CODE", "PARENT_CLIENT_ID",
      "MANAGED_BY_PARENT_CLIENT_ID" , "ULTIMATE_PARENT_ID"  ,"INTS_STATUS" ,  "TECHNICAL_STATUS" , "ON_BOARD_DATE" , "CEASED_DATE"  )

    println(" All File Count= "+clientFileWithSelectedColumns.count())

    // Filter based on Entity_type == party and Client_Type_code == Legal
    var companyData = clientFileWithSelectedColumns.
      filter(clientFileWithSelectedColumns.col("ENTITY_TYPE"). === ("PARTY")).
      filter(clientFileWithSelectedColumns.col("CLIENT_TYPE_CODE"). === ("LEGAL"))



    var fundData = clientFileWithSelectedColumns.
      filter(clientFileWithSelectedColumns.col("ENTITY_TYPE"). === ("FUND")).
      filter(clientFileWithSelectedColumns.col("CLIENT_TYPE_CODE"). === ("UNIT"))

    companyData.show()
    fundData.show()

    companyData = companyData.as("CLIENT_TABLE")
    fundData = fundData.as("FUND_TABLE")

    val joinedDF = companyData.
      join(fundData, col("CLIENT_TABLE.CLIENT_ID") === col("FUND_TABLE.ULTIMATE_PARENT_ID"),"inner")

    joinedDF.select(
      col("CLIENT_TABLE.CLIENT_ID"),
      col("FUND_TABLE.ULTIMATE_PARENT_ID"),
      col("FUND_TABLE.CLIENT_ID"), // FUND_ID
      col("CLIENT_TABLE.INTS_STATUS"),
      col("CLIENT_TABLE.ON_BOARD_DATE"),
      col("CLIENT_TABLE.CEASED_DATE")
    ).show

    /*val joinedDF = companyData.
      join(fundData,
        joinedDF.select(col("CLIENT_ID"), col("ISO_LEGAL_NAME"), col("ULTIMATE_PARENT_ID"), col("CLIENT_ID"))
        companyData.col("CLIENT_ID") === fundData.col("ULTIMATE_PARENT_ID"),"inner").show*/






  }

}

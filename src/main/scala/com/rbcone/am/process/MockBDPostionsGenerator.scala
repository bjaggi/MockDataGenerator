package com.rbcone.am.process

import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object MockBDPostionsGenerator {

  def main(args: Array[String]) {
    val session=SparkSession.builder().
      appName("MockData_Generator").config("spark.sql.crossJoin.enabled","true")
      .config("mapred.output.compress", false)
      // .config("spark.debug.maxToStringFields","true")
      //.master("local[*]")
      .getOrCreate()

    def replaceTo(inputStr:String) = {
      import org.apache.spark.sql.functions.udf
      udf((doctype:String) => {

        inputStr
      })
    }

    val bdPositions = session.read.option("header", "true").csv("hdfs:////output_focus_files/bd_positions")
    bdPositions.withColumn("c_doc_type_8",replaceTo("DELTA6")(bdPositions("c_doc_type_8") ))
    ///hive/external/bdpositions").toDF() //  read sample data from a file

    val bdStructure = bdPositions.select("*").schema // copied the table structure for reference
    val rows = new java.util.ArrayList[Row]
    val schema = session.createDataFrame(rows, bdStructure)
    var newBdPositions = session.createDataFrame(bdPositions.toJavaRDD, bdStructure)//bdPositions
//    val dateAccum  = session.sparkContext.longAccumulator("DateIncrementer" )
    schema.show()

    //val tempBdPositions = bdPositions.withColumn("c_doc_type_8",replaceTo("DELTA6")(bdPositions("c_doc_type_8") ))
    bdPositions.show(3)
    //tempBdPositions.show(3)


    newBdPositions.coalesce(1).write.mode(SaveMode.Overwrite).csv("hdfs:///output_focus_files/bd_positions")



  }

  }

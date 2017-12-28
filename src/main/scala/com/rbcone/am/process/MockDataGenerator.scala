package com.rbcone.am.process

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.{DateTime, Days}

/**
  * Created by ${Brijesh_Jaggi} on 2017/12/27.
  */
object MockDataGenerator {

  def main(args: Array[String]){
    System.setProperty("hadoop.home.dir","C:\\Softwares\\Winutils")
    val start = DateTime.now.minusDays(0)
    val end   = DateTime.now.plusDays(2) // to be changed to 30 for entire month of dates starting today

    val conf=new SparkConf().setAppName("MockDataGenerator").setMaster("local[*]").set("spark.sql.crossJoin.enabled","true")
    Logger.getLogger("org").setLevel(Level.INFO)

    val session=SparkSession.builder().
      appName("MockData_Generator").config("spark.sql.crossJoin.enabled","true")
     // .config("spark.debug.maxToStringFields","true")
      .master("local[*]").getOrCreate()

    import session.implicits._
    val bdPositions = session.read.option("header", "true").csv("input_focus_files/bd_positions.csv").toDF() //  read sample data from a file
    val bdStructure = bdPositions.select("*").schema // copied the table structure for reference
    val rows = new java.util.ArrayList[Row]
    val schema = session.createDataFrame(rows, bdStructure)
    //schema.show()
    var globalPlusDays= new org.joda.time.DateTime
    val numberOfDays = Days.daysBetween(start, end).getDays()
    var newBdPositions = session.createDataFrame(bdPositions.toJavaRDD, bdStructure)//bdPositions

    // i couldnt pass String to this column datatype so did a temp fix ( Dont know the correct syntax :(  )
    def nextDateFunc = udf {(dateStr: String) =>
      //"2/2/2017"
      globalPlusDays.toString(DateTimeFormat.forPattern("dd/MM/yyyy"))
    }


    //loop for all days
    for (plusDays <- 0 to numberOfDays) yield {
      val nextDate =  start.plusDays(plusDays)
      println("Processing Date =  " ,nextDate.toString)
      globalPlusDays = nextDate // set the next iterating date
      val tempBdPositions = bdPositions.withColumn("d_pos", nextDateFunc(bdPositions("d_pos") ))
      newBdPositions= newBdPositions.union(tempBdPositions)

     /*
     //Option2
     val tempBdPositions2 = bdPositions.withColumn("d_pos", nextDateFunc(bdPositions("d_pos") ))
      tempBdPositions2.show()

      var dataRDD = session.emptyDataFrame
      //pass rdd and schema to create dataframe

      var tempBdPositions = session.createDataFrame(bdPositions.toJavaRDD, bdStructure)//bdPositions
      tempBdPositions= tempBdPositions.union(tempBdPositions2)
      tempBdPositions2.persist()
      tempBdPositions.persist()

      //var appended = bdPositions.union(newBdPositions)
      newBdPositions= newBdPositions.union(tempBdPositions).persist()*/
    }




//    newBdPositions.select(newBdPositions("d_pos")).distinct().show()
    newBdPositions.show()


    // Created Dummy Data as the code isnt working
    globalPlusDays = new DateTime(2222, 2, 22, 0, 0, 0, 0);
    val df2 = bdPositions.withColumn("d_pos", nextDateFunc(bdPositions("d_pos") ))
    df2.show()
    globalPlusDays = new DateTime(3333, 3, 3, 0, 0, 0, 0);
    val df3 = bdPositions.withColumn("d_pos", nextDateFunc(bdPositions("d_pos") ))
    df3.show()

    bdPositions.unionAll(df2).persist().unionAll(df3).persist.show()




  }


}

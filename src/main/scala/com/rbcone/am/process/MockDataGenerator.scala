package com.rbcone.am.process

import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DateType
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.{DateTime, Days}
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.DateTime
import scala.util.Random

/**
  * Created by ${Brijesh_Jaggi} on 2017/12/27.
  */
object MockDataGenerator {

  def main(args: Array[String]){
    //System.setProperty("hadoop.home.dir","C:\\Softwares\\Winutils")
    var numOfDays = 0
      if (args.isEmpty || Option(args{0}).getOrElse("").isEmpty){
        numOfDays = 30
      } else {
        numOfDays = args{0}.toInt
      }
    println("numOfDays= ",numOfDays)

    val start = DateTime.now.minusDays(0)
    val end   = DateTime.now.plusDays(numOfDays) // to be changed to 30 for entire month of dates starting today
    val numberOfDays = Days.daysBetween(start, end).getDays()

    val session=SparkSession.builder().
      appName("MockData_Generator").config("spark.sql.crossJoin.enabled","true")
     // .config("spark.debug.maxToStringFields","true")
      //.master("local[*]")
      .getOrCreate()

    import session.implicits._
    val bdPositions = session.read.option("header", "true").csv("hdfs:///input_focus_files/bd_positions.csv").toDF() //  read sample data from a file
    val bdStructure = bdPositions.select("*").schema // copied the table structure for reference
    val rows = new java.util.ArrayList[Row]
    val schema = session.createDataFrame(rows, bdStructure)
    var newBdPositions = session.createDataFrame(bdPositions.toJavaRDD, bdStructure)//bdPositions
    val dateAccum  = session.sparkContext.longAccumulator("DateIncrementer" )
    schema.show()

    def nextDateFunc2(dateAcumParam: Int) = {
      import org.apache.spark.sql.functions.udf
      udf((dPosDate:String) => {
        val formatter = DateTimeFormat.forPattern("dd/MM/yyyy")
        val dt = formatter.parseDateTime(dPosDate)
        //println("dPosDate",dPosDate,"dateAccum=",dateAcumParam,"new date by udf:",dt.plusDays(dateAcumParam).toString(DateTimeFormat.forPattern("dd/MM/yyyy")))
        dt.plusDays(dateAcumParam).toString(DateTimeFormat.forPattern("dd/MM/yyyy"))
      })
    }


    //loop for all days
    for (plusDays <- 0 to numberOfDays)  {
      val nextDate =  start.plusDays(plusDays)
      dateAccum.add(1)
      val tempBdPositions = bdPositions.withColumn("d_pos",nextDateFunc2(dateAccum.value.toInt)(bdPositions("d_pos") ))
      dateAccum.add(1)
      val tempBdPositions2 = bdPositions.withColumn("d_pos",nextDateFunc2(dateAccum.value.toInt)(bdPositions("d_pos") ))

      newBdPositions= newBdPositions.union(tempBdPositions.union(tempBdPositions2))
    }




//    newBdPositions.select(newBdPositions("d_pos")).distinct().show()
    newBdPositions.show(10)
newBdPositions.write.format("csv").save("hdfs:///output_focus_files/bd_positions.csv")

  }


}

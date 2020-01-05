package com.github.rajens.advance

import org.apache.log4j._
import org.apache.spark._

import scala.math.{max, min}

/* Finds the Maximum and Minimum Temparation recorded for for given year and weather station */
object MinMaxTemprature extends App {

  def parseLine(lines : String)= {

    val fields = lines.split(",");
    val stationID = fields(0);
    val tempType = fields(2);
    val temprature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f;

    (stationID,tempType,temprature);

  }

  override def main(args: Array[String]): Unit = {

    Logger.getLogger("MaxTemprature").setLevel(Level.ERROR);

    //create spark context
    val sc = new SparkContext("local[2]", "MaxTemprature" );

    //read file
    val lines = sc.textFile("in/temprature.csv");

    //parse the line , split by (,) and extract values in fields
    //Create RDD from (StationId, TempType, TempValue) where StationID is the KEY
    //Filter the RRD for TempType = 'TMAX'

    val maxfilteredRdd  = lines.map(parseLine).filter(x =>  x._2 == "TMAX" );

    val minfilteredRdd  = lines.map(parseLine).filter(x =>  x._2 == "TMIN" );

    //Create New RDD with (StationId, TempValue) where StationID is the KEY to reduce the size of dataset
    //Reduce the RDD my StationID KEY maintaining Max Temp
    val maxTempRdd = maxfilteredRdd.map(x => (x._1 , x._3.toFloat)).reduceByKey((x,y) => max(x,y));

    //Create New RDD with (StationId, TempValue) where StationID is the KEY to reduce the size of dataset
    //Reduce the RDD my StationID KEY maintaining Min Temp
    val minTempRdd = minfilteredRdd.map(x => (x._1 , x._3.toFloat)).reduceByKey((x,y) => min(x,y));

    // Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
    val maxResults = maxTempRdd.collect()
    val minResults = minTempRdd.collect();

    // Sort and print the final results
    for ( result <- minResults.sorted) {
      val station = result._1;
      val temp = result._2;
      val fommatedTemp = f"$temp%.2f F";
      println(s"$station mimimum temprature: $fommatedTemp");

    }
    // Sort and print the final results
    for ( result <- maxResults.sorted) {
      val station = result._1;
      val temp = result._2;
      val fommatedTemp = f"$temp%.2f F";
      println(s"$station maximum temprature: $fommatedTemp");

    }



  }

}

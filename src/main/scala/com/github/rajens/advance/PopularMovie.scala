package com.github.rajens.advance

import java.nio.charset.CodingErrorAction

import org.apache.log4j._
import org.apache.spark._

import scala.io.{Codec, Source}

/* Find the popular movie based on number of ratings */
object PopularMovie extends App {

 def loadMovieNames(): Map[Int,String] = {

  var movieNamesMap : Map[Int,String] = Map()
  implicit val codec = Codec("UTF-8")
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

  var lines = Source.fromFile("in/ml-100k/u.item").getLines()

  for (line <- lines){
   var fields = line.split('|')
   if(fields.length>1){
    movieNamesMap += (fields(0).toInt -> fields(1))
   }
  }

  return movieNamesMap;
 }

 override def main(args: Array[String]): Unit = {

  Logger.getLogger("PopularMovie").setLevel(Level.ERROR);

  val sc = new SparkContext("local[2]","PopularMovie");

  //Broadcast variable to publish the Movie name map
  val movieNames = sc.broadcast(loadMovieNames)

  //println(s"Movie Name: ${movieNames.value} ")

  //read u.data file - fileds, UserId, MovieId, Rating, Timestamp
  val lines = sc.textFile("in/ml-100k/u.data");

  //create RDD with Movie ID to count the number of occurances
  val rdd = lines.map( x => (x.split("\\t")(1).toInt,1))

  //reduce by Movie Id - count the occrance and sort by count
  val moviesRdd = rdd.reduceByKey((x,y) => x+y).map(x => (x._2,x._1)).sortByKey(false);

  //Map Movie Names with Movie Id and replace Ids with Names
  val movieNameRdd = moviesRdd.map( x => (movieNames.value(x._2),x._1))

  //collect
  val results = movieNameRdd.collect();

  //print
  //results.foreach(println);
  // Sort and print the final results
  for ( result <- results) {
   val movieName = result._1;
   val ratings = result._2;

   println(s"Movie Name:$movieName" + "---"
     + "Ratings:$ratings");
  }


  }

}

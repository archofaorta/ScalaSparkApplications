package com.github.rajens.advance

import org.apache.log4j._
import org.apache.spark._

/* Find most popular superhero based on the connection graph - hero with max number of connections */

object PopularSuperHero extends App {

  //Constuct tuple (Id, total connections)
 def parseGraph (lines : String )  = {

   var fields = lines.split("\\s+")

   val movieId = fields(0).toInt
   val totalConnections = (fields.length - 1)

   (movieId, totalConnections)

 }

 /*Construct tuple with ID-> Name key/Value pair (Option object with some value, none in case of null or no value)
    Flatmap extracts value from Some object and discards None values
  */
 def parseNames (lines : String) : Option[(Int,String)] = {

  var fields = lines.split('\"');
  if (fields.length > 1) {
    return Some(fields(0).trim().toInt, fields(1))
  }
  else {
    return None
  }
 }

  /* Main Funtions where actions happens */
  override def main(args: Array[String]): Unit = {

    Logger.getLogger("PopularSuperHero").setLevel(Level.ERROR)

    //Spark Context
    val sc = new SparkContext("local[2]", "PopularSuperHero")

    //Read Super-hero Names file and create RDD mapping Hero ID -> Names
    val names = sc.textFile("in/Marvel-names.txt")
    val nameRDD = names.flatMap(parseNames)

    //Read Super-hero graph file and create RDD mapping hero ID -> size of graph (# of connections)
    val graph = sc.textFile("in/Marvel-graph.txt")
    val graphRDD = graph.map(parseGraph)

    //Count total connections by Id, Sort the RDD based on total count, reterive the Id with Maximum count
    val totalConnectionsRDD = graphRDD.reduceByKey((x,y) => x + y).map( x => (x._2, x._1)).sortByKey(false).max()

    //Lookup Superhero Name with ID from Names RDD
    val mostPopular = nameRDD.lookup(totalConnectionsRDD._2)(0)

    //Print the result
    println(s"$mostPopular is Most Popular Super Hero With ${totalConnectionsRDD._1} Connections")
 }

}

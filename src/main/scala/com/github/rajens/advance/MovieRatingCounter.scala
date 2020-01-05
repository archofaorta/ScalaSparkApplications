package com.github.rajens.advance

import org.apache.log4j._
import org.apache.spark._

/* Counts the total number of movies grouped by its ratings */
object MovieRatingCounter extends App{

  /** main function where the action happens */
  override def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "RatingsCounter")

    val lines = sc.textFile("in/ml-100k/u.data")

    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val ratings = lines.map(x => x.toString().split("\t")(2))

    // Count up how many times each value (rating) occurs
    val results = ratings.countByValue()

    // Sort the resulting map of (rating, count) tuples
    val sortedResults = results.toSeq.sortBy(_._1)

    // Print each result on its own line.
    sortedResults.foreach(println)
  }
}

package com.github.rajens.advance

import org.apache.log4j._
import org.apache.spark._

/** Computes the average number of friends by name from social networking dataset */
object FriendsByName extends App {
  
  /** A function that splits a line of input into (name, numFriends) tuples. */
  def parseLine(line: String) = {
      // Split by commas
      val fields = line.split(",")
      // Extract the Name and numFriends fields
      val name = fields(1)
      val numFriends = fields(3).toInt
      // Create a tuple that is our result.
      (name, numFriends)
  }
  
  /** main function where the action happens */
  override def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "FriendsByName")
  
    // Load each line of the source data into an RDD
    val lines = sc.textFile("in/friends_demographic.csv")
    
    // Use our parseLines function to convert to (name, numFriends) tuples
    val rdd = lines.map(parseLine);
    
    // Create RDD of form (name, numFriends) where name is the KEY and numFriends is the VALUE
    // mapValues to convert each numFriends value to a tuple of (numFriends, 1)
    // reduceByKey to sum up the total numFriends and total instances for each name
    // add together all the numFriends values and 1's respectively.
    val totalsByName = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2));
    
    // Output - tuples of (name, (totalFriends, totalInstances))
    // To compute the average divide totalFriends / totalInstances for each name.
    val averagesByName = totalsByName.mapValues(x  => x._1 / x._2);
    
    // Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
    val results = averagesByName.collect()
    
    // Sort and print the final results.
    results.sorted.foreach(println)
  }
    
}
  
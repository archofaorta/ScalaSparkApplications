package com.github.rajens.advance

import org.apache.log4j._
import org.apache.spark._

/* Finds total spending for customers based on order data set */
object CustomerSpending extends App{

  def parseLine(lines : String)  = {

    val fields = lines.split(",");
    val customerID = fields(0).toInt;
    val orderValue = fields(2).toFloat;

    (customerID,orderValue);
  }

  override def main(args: Array[String]): Unit = {

    Logger.getLogger("CustomerSpending").setLevel(Level.ERROR);

    val sc = new SparkContext("local[2]", "CustomerSpending");

    //Read Customer Order File - Customer ID, Item ID, Order Value
    val lines = sc.textFile("in/customer-orders.csv");

    val parsedLines = lines.map(parseLine);

    val totalOrderVal = parsedLines.reduceByKey((x,y)=> x+y);

    val orderedRDD = totalOrderVal.map( x => (x._2, x._1));

    for (value <- orderedRDD.sortByKey(true)) {
      val custId = value._2;
      val orderTotal = value._1;
      println(s"Customer: $custId  OrderTotal: $orderTotal");
    }



  }

}

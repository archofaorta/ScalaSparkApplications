package com.github.rajens.basic

object FlowControl extends  App {

  // If / else syntax
  if (1 > 3) println("Impossible!") else println("It makes sense.")
  //> The world makes sense.

  if (1 > 3) {
    println("Impossible!")
  } else {
    println("It makes sense.")
  }

  // Matching - like switch in other languages:
  val number = 3                                  //> number  : Int = 3
  number match {
    case 1 => println("One")
    case 2 => println("Two")
    case 3 => println("Three")
    case _ => println("Something else")
  }                                         //> Three

  // For loops
  for (x <- 1 to 4) {
    val squared = x * x
    println(squared)
  }


  // While loops
  var x = 10                                      //> x  : Int = 10
  while (x >= 0) {
    println(x)
    x -= 1
  }                                               //> 10

  x = 0
  do { println(x); x+=1 } while (x <= 10)         //> 0

  // Expressions
  // "Returns" the final value in a block automatically

  {val x = 10; x + 20}                           //> res0: Int = 30

  println({val x = 10; x + 20})            //> 30

    //Fibo ( 1 - 10),  1, 1, 2, 3, 5, 8, 13, 21,



}

package com.icanindya.linkage

object Test {
  def main(args: Array[String]): Unit = {
    val list = List(6, 1, 5, 9)
    println(list.sortBy {x => x > x})
    println(list.sortWith(_>_))
  }
}
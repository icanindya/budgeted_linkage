package com.icanindya.linkage

class AdjacencyList(dsAttrs: Array[List[String]]) {
  
  def get(): Map[Int, List[Int]] = {
    var adjList = Map[Int, List[Int]]()
    for (i <- 0 to dsAttrs.length - 1) {
      val neighbors = for {
        j <- 0 to dsAttrs.length - 1
        if i != j
        if dsAttrs(i).intersect(dsAttrs(j)).length != 0
      } yield j
      adjList += (i -> neighbors.toList)
    }
    adjList    
  }
  
}
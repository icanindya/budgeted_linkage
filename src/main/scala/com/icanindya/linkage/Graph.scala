package com.icanindya.linkage

import scala.collection.mutable.Stack
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


class Graph[T](val adjList: Map[T, List[T]]){
  
  val path = Stack[T]()
  val onPath = ListBuffer[T]()
  
  val allPaths = ListBuffer[Array[T]]()
  
  def allPaths(s: T, t: T)(implicit ev: ClassTag[T]): List[Array[T]] = {
    enumerate(s, t)
    allPaths.toList
  }

  def enumerate(v: T, t: T)(implicit ev: ClassTag[T]): Unit = {
    path.push(v)
    onPath += v
    
    if(v.equals(t)){
      allPaths += path.reverse.toArray
    }
    else{
      for(w <- adjList(v)){
        if(!onPath.contains(w)) enumerate(w, t)
      }
    }
    path.pop()
    onPath.remove(onPath.indexOf(v))
  }
}
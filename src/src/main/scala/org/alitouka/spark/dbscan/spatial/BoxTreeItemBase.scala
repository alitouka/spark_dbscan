package org.alitouka.spark.dbscan.spatial

import scala.collection.mutable.ArrayBuffer


private [dbscan] abstract class BoxTreeItemBase [T <: BoxTreeItemBase[_]] (val box: Box) extends Serializable {
  var children: List[T] = Nil

  def flatten [X <: BoxTreeItemBase[_]]: Iterable[X] = {
    this.asInstanceOf[X] :: children.flatMap ( x => x.flatten[X] ).toList
  }

  def flattenBoxes: Iterable[Box] = {
    flatten [BoxTreeItemBase[T]].map { x => x.box }
  }

  def flattenBoxes (predicate: T => Boolean): Iterable [Box] = {
    val result = ArrayBuffer[Box] ()

    flattenBoxes(predicate, result)

    result
  }

  private def flattenBoxes [X <: BoxTreeItemBase[_]] (predicate: X => Boolean, buffer: ArrayBuffer[Box]): Unit = {

    if (!children.isEmpty && children.exists ( x => predicate (x.asInstanceOf[X]))) {
      children.foreach ( x => x.flattenBoxes[X](predicate, buffer))
    }
    else {
      buffer += this.box
    }
  }
}

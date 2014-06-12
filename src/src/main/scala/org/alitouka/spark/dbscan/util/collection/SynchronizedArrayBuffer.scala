package org.alitouka.spark.dbscan.util.collection

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable


private [dbscan] class SynchronizedArrayBuffer [T] extends ArrayBuffer[T] with mutable.SynchronizedBuffer[T] {

}

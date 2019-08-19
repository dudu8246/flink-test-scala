package com.chris.flink.test01

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingWCScalaApp {

  /**
   * Flink APP by Scala
   */

  def main(args: Array[String]): Unit = {

    val env =StreamExecutionEnvironment.getExecutionEnvironment

    // start netcat => nc -lk 9999
    val text = env.socketTextStream("localhost",9100)

    import org.apache.flink.api.scala._
    val windowCounts = text.filter(_ != "\\,").flatMap(_.split(" "))
      .map(x => WC(x,1))
      .keyBy("word")
      .timeWindow(Time.seconds(5))
      .sum("count")


    windowCounts.print().setParallelism(1)
    println(windowCounts)
    env.execute("StreamingWCScalaApp")




  }

  case class WC(word : String, count : Int)




}

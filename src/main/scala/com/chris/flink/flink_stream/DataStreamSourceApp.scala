package com.chris.flink.flink_stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object DataStreamSourceApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //socketFunction(env)

    nonParallelSourceFunction(env)
    //ParallelSourceFunction(env)
    env.execute("DataStreaming App")
  }


  def ParallelSourceFunction(env: StreamExecutionEnvironment): Unit ={

    val data = env.addSource(new CustomSourceFunction).setParallelism(2)
    data.print().setParallelism(1)
  }

  def nonParallelSourceFunction(env: StreamExecutionEnvironment): Unit ={

    val data = env.addSource(new CustomNonParallelSourceFunction)
    data.print()
  }


  def socketFunction(env:StreamExecutionEnvironment): Unit ={

    val data  = env.socketTextStream("local", 9999)
    data.print()
  }



}

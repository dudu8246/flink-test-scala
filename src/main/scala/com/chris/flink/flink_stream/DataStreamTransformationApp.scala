package com.chris.flink.flink_stream
import java.lang
import java.util

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.collector.selector.OutputSelector


object DataStreamTransformationApp {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //filterFunction(env)

    splitSelectFunction(env)
    env.execute("count DataSource")
  }

  def splitSelectFunction(env:StreamExecutionEnvironment): Unit ={

    val data = env.addSource(new CustomSourceFunction).setParallelism(1)

    val splits = data.split(new OutputSelector[Long] {
      override def select(value: Long): lang.Iterable[String] = {

        val list = new util.ArrayList[String]()

        if (value %2  == 0)
          list.add("even")
          else
          list.add("odd")

          list
      }
    })

    splits.select("odd").print().setParallelism(1)
  }



  def filterFunction(env: StreamExecutionEnvironment): Unit ={

    //利用SourceFunction来造数据  通过这些数据在本地测试
    val data = env.addSource(new CustomSourceFunction)

    data.map({x =>
      //算子中可以使用{} 的方式去在其中定义方法，比如print看看该算子具体执行的是什么操作
      println("recevied :"+x)
      x
    }).filter(_%2 ==0).print().setParallelism(1)

  }





}

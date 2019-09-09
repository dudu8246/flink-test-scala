package com.chris.flink.flink_start

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * 基于Flink开发计数器
 */
object CounterApp {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = env.fromElements("hadoop","spark","flink","pyspark","storm")

    val info = data.map(new RichMapFunction[String ,String]() {

      // step1: 定义计数器
      val counter = new LongCounter()

      //step2 : 注册计数器
      override def open(parameters: Configuration): Unit = {

      getRuntimeContext.addAccumulator("count-test", counter)
    }

      override def map(in: String): String = {
        counter.add(1)
        in
      }
    })

    info.writeAsText("file:///home/ccbh/chris-workspace/data/sink-scala-count/",WriteMode.OVERWRITE)
      .setParallelism(3)

    val jobResult = env.execute("CounterApp")
    //获取计数器
    val num = jobResult.getAccumulatorResult[Long]("count-test")

    println("num:"+num)


  }



}

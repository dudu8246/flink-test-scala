package com.chris.flink.flink_stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object SinkToMySQL {


  //case class student(id:Int ,name:String,age:Int)


  def main(args: Array[String]): Unit = {
    val  env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = env.socketTextStream("localhost",9000)

    val student = data.map(x => {

      var a = x.split(",")
      (a(0).toInt,a(1),a(2).toInt)

    })

    student.addSink(new SinkApp)


    env.execute("SinkToMySQL")

  }



}

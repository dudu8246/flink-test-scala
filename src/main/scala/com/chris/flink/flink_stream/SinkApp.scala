package com.chris.flink.flink_stream

import java.sql.{Connection, PreparedStatement}

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

class SinkApp extends RichSinkFunction[(Int,String,Int)]{

  var conn:Connection = _
  var pres:PreparedStatement = _
  var username = "root"
  var dburl = "jdbc:mysql://localhost:3306/chris_flink"
  var sql = "insert into words("

  override def open(parameters: Configuration): Unit ={




  }
}

package com.chris.flink.flink_stream

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class SinkApp extends RichSinkFunction[(Int,String,Int)]{

  var conn:Connection = _
  var pres:PreparedStatement = _
  var username = "root"
  var dburl = "jdbc:mysql://localhost:3306/flink_test"
  var password = "root"
  var sql = "insert into students (id,name,age) values (?,?,?)"


  override def invoke(value:(Int,String,Int) ) {

    pres.setInt(1,value._1);
    pres.setString(2, value._2);
    pres.setInt(3,value._3);

    pres.executeUpdate();
    println("values ：" +value._1+"--"+value._2+"--"+value._3);
  }
  override def open( parameters:Configuration) {
    Class.forName("com.mysql.jdbc.Driver");
    conn = DriverManager.getConnection(dburl, username, password);
    pres = conn.prepareStatement(sql);
    super.close()
  }
  override def close() {
    pres.close();
    conn.close();
  }

}

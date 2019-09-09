package com.chris.flink.flink_start

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

object DistributedCacheApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val filePath = "file:///home/ccbh/chris-workspace/data/hello.txt"

    //step1:注册一个本地的HDFS文件
    env.registerCachedFile(filePath,"scala-dc")

    val data = env.fromElements("hadoop","spark","flink","pyspark","storm")

    data.map(new RichMapFunction[String,String](){
      //step2:在open方法中获取到分布式缓存的内容
      override def open(parameters: Configuration): Unit = {

        val dcFile = getRuntimeContext.getDistributedCache().getFile("scala-dc")
        ///common.io中的FileUtils
        val lines = FileUtils.readLines(dcFile)

        /**
         * 这里需要 import scala.collection.JavaConversions._ 解决Scala集和 与 Java集和不兼容问题
         *
         */

        import scala.collection.JavaConverters._
        for(ele <- lines.asScala){
          println(ele)
        }
      }
      override def map(value: String): String = {value}
    }).print()


    //env.execute("DistributedCache")


  }



}

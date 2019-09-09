package com.chris.flink.flink_start

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode


object DataSetSinkApp {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = 1.to(10)
    val text = env.fromCollection(data)
    val filePath = "/home/ccbh/chris-workspace/data/sink-out"

    //WriteMode参数：如果不使用OVERWRITE模式 会自己以该目录名创建一个文件并且每次都必须删除该文件才能执行
    //  OVERWRITE模式则是创建一个目录然后根据并行度创建 1 2 3 等几个文件成装结果
    text.writeAsText(filePath,WriteMode.OVERWRITE).setParallelism(3)

    env.execute("DataSetSinkApp")
  }




}

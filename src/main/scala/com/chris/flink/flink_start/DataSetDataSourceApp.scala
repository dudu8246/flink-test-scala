package com.chris.flink.flink_start

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

      object DataSetDataSourceApp {

        def main(args: Array[String]): Unit = {

          val env  =  ExecutionEnvironment.getExecutionEnvironment

          //fromCollection(env)
          //textFile(env)
          //csvFile(env)
          //readRecursiveFile(env)
          readCompressionFiles(env)
        }


        def readCompressionFiles(env:ExecutionEnvironment)={
          val filePath ="file:///home/ccbh/chris-workspace/data/nested.tar.gz"
          env.readTextFile(filePath).print()
        }



        def readRecursiveFile(env:ExecutionEnvironment)={

          val filePath = "file:///home/ccbh/chris-workspace/data/nested"

          val parameter = new Configuration
          parameter.setBoolean("recursive.file.enumeration",true)

          env.readTextFile(filePath).withParameters(parameter).print()

        }





        def csvFile(env:ExecutionEnvironment)={
          val filePath = "file:///home/ccbh/chris-workspace/data/people.csv"
          case class MyCaseClass(name:String ,age:Int)

          //env.readCsvFile[(String,Int,String)](filePath,ignoreFirstLine = true).print()
          //env.readCsvFile[(String,Int,String)](filePath,ignoreFirstLine = true,includedFields = Array(0,1)).print()
          env.readCsvFile[MyCaseClass](filePath,ignoreFirstLine = true,includedFields = Array(0,1)).print()

        }

        def textFile(env:ExecutionEnvironment): Unit={
          ///DataSet by TextFile (The path can be a dictionary)
          val filePath = "file:///home/ccbh/chris-workspace/data/hello.txt"

          env.readTextFile(filePath).print()


        }


        def fromCollection(env:ExecutionEnvironment): Unit= {
          //Dataset by collector
          val data = 1 to 10
          env.fromCollection(data).print()


        }

}



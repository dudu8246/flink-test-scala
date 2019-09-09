package com.chris.flink.flink_start

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

object DataSetTransformationApp {

  var listData = List(1,2,3,4,5,6,7,8,9,10)
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //mapPartitionFunction(env)
    //firstFunction(env)
    //flatMapFunction(env)
    //distinctFunction(env)
    //joinFunction(env)
    //crossFunction(env)
    joinTest(env)

  }


  def joinTest(env:ExecutionEnvironment): Unit ={
    println("start------------------------------")
    val info1 = ListBuffer[(String,String)]() //编号 名字
    info1.append(("a1", "a123"));
    info1.append(("b2", "b123"));
    info1.append(("c3", "c133"));
    info1.append(("d4", "d123"));

    val info2 = ListBuffer[(String,String)]()  //编号 城市
    info2.append(("a1","a123"));
    info2.append(("b2","b123"));
    info2.append(("c3","c123"));
    info2.append(("d44","d123"));

    val data1 =env.fromCollection(info1)
    val data2 =env.fromCollection(info2)
    //  where条件指定左边数据的key，equalTo指定右边数据的key（可以选字段名，也可以选择index）
    //  利用apply方法  (a,b） => { a._1, b.2}  其中 a，b 就分别代表 经过join后的 左边的数据 和右边的数据
    //  通过 a._1 这种方式 选择其中的第几个元素展
    //  如果用OuterJoin方法 就要判断会不会出现元素为空的情况
    //  比如leftOuterJoin时 右边data中就可能会出现空的情况，此时就应该加if语句 替换用“-”掉右边的所有元素
    //  而fullOuterJoin时  可能左右都为空  就需要左右都加判断
    data1.leftOuterJoin(data2).where(0).equalTo(0).apply((first,second) => {

      if (second == null)  (first._1,first._2,"-")  else if (first._2 == second._2) ("-","-","-")else  (first._1,first._2,second._2) }).print()

//    data1.fullOuterJoin(data2).where(0).equalTo(0).apply((first,second) => {
//
//      if (second == null)  (first._1,first._2,"-")  else  if (first == null) (second._1,"-",second._2)
//      else (first._1,first._2,second._2)
//
//    }).print()

  }


  def mapFunction(env:ExecutionEnvironment)={
    val data = env.fromCollection(listData)
    //data.map((x:Int) => x+1).print()
    //data.map(x => x+1)
    data.map(_ + 1).filter(_>5)

  }
  //DataSource 100个元素，把结果写到数据库中
  def mapPartitionFunction(env:ExecutionEnvironment)={
    val data = env.fromCollection(listData)

    var students = 1 to 100
    //mapPartition:设置并行度 如果用map每个元素都会作一次操作，而mapPartition则根据分区来调用操作大大提高性能
    val data1 = env.fromCollection(students).setParallelism(4)
    data1.mapPartition(x => {

      x
    }).print()


  }

  def firstFunction(env:ExecutionEnvironment): Unit ={
    val info = ListBuffer[(Int,String)]()
    info.append((1,"Hadoop"));
    info.append((1,"Spark"));
    info.append((1,"Flink"));
    info.append((2,"Java"));
    info.append((2,"Spring Boot"));
    info.append((3,"Linux"));
    info.append((4,"VUE"));

    val data = env.fromCollection(info)

    //data.first(3).print()
    data.groupBy(0).sortGroup(1,Order.DESCENDING).first(1).print()

  }

  def flatMapFunction(env:ExecutionEnvironment): Unit ={
    val info = ListBuffer[String]()
    info.append("hadoop,spark")
    info.append("hadoop,flink")
    info.append("flink,flink")

    val data =env.fromCollection(info)

    data.flatMap(_.split(",")).map((_,1)).groupBy(0).sum(1).print()

  }

  def distinctFunction(env:ExecutionEnvironment): Unit ={
    val info = ListBuffer[String]()
    info.append("hadoop,spark")
    info.append("hadoop,flink")
    info.append("flink,flink")

    val data =env.fromCollection(info)

    data.flatMap(_.split(",")).distinct().print()

  }

  def joinFunction(env:ExecutionEnvironment): Unit ={
    println("start------------------------------")
    val info1 = ListBuffer[(Int,String)]() //编号 名字
      info1.append((1, "chris"));
      info1.append((2, "雷斯"));
      info1.append((3, "周笔"));
      info1.append((4, "六七儿"));

    val info2 = ListBuffer[(Int,String)]()  //编号 城市
    info2.append((1,"长春"));
    info2.append((5,"苏州"));
    info2.append((3,"杭州"));
    info2.append((4,"山东"));
    println("start transformation ---------------------------")
    val data1 =env.fromCollection(info1)
    val data2 =env.fromCollection(info2)
    //  where条件指定左边数据的key，equalTo指定右边数据的key（可以选字段名，也可以选择index）
    //  利用apply方法  (a,b） => { a._1, b.2}  其中 a，b 就分别代表 经过join后的 左边的数据 和右边的数据
    //  通过 a._1 这种方式 选择其中的第几个元素展
    //  如果用OuterJoin方法 就要判断会不会出现元素为空的情况
    //  比如leftOuterJoin时 右边data中就可能会出现空的情况，此时就应该加if语句 替换用“-”掉右边的所有元素
    //  而fullOuterJoin时  可能左右都为空  就需要左右都加判断
    data1.leftOuterJoin(data2).where(0).equalTo(0).apply((first,second) => {

      if (second == null)  (first._1,first._2,"-")  else  (first._1,first._2,second._2) }).print()

    data1.fullOuterJoin(data2).where(0).equalTo(0).apply((first,second) => {

      if (second == null)  (first._1,first._2,"-")  else  if (first == null) (second._1,"-",second._2)
      else (first._1,first._2,second._2)

    }).print()

    println("out -------------------------------------------")
  }

  def crossFunction(env:ExecutionEnvironment): Unit ={
    val info1 = List("曼联","曼城")
    val info2 = List(3,1,0)

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)
    data1.cross(data2).print()

  }




}

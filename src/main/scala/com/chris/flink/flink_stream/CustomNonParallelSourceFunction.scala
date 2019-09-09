package com.chris.flink.flink_stream

import org.apache.flink.streaming.api.functions.source.SourceFunction

class CustomNonParallelSourceFunction extends SourceFunction[String]{

  var count  = List("1,jack,25","2,bob,20","3,wangxiang,22")

  var isRunning = true
  var num =0
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {

    while (isRunning) {
      ctx.collect(count(num))
      num+=1
      if (num==count.length) cancel()
      Thread.sleep(2000)
    }

  }


  override def cancel(): Unit = {
    isRunning = false

  }
}

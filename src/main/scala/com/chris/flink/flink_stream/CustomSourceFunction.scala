package com.chris.flink.flink_stream

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

class CustomSourceFunction extends ParallelSourceFunction[Long]{

  var isRunning = true
  var count = 1L

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning == true){
      ctx.collect(count)
      count+=1
      Thread.sleep(1000)

    }

  }

  override def cancel(): Unit = {

    isRunning = false
  }
}

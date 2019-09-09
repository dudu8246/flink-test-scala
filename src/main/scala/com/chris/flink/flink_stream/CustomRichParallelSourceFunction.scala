package com.chris.flink.flink_stream

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

class CustomRichParallelSourceFunction extends RichParallelSourceFunction {
  override def run(ctx: SourceFunction.SourceContext[Nothing]): Unit = ???

  override def cancel(): Unit = ???
}

package com.knoldus.flink.streaming.scala.examples

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

/**
  * Implements the "WordCount" program that computes a simple word occurrence histogram over streaming data from Kafka.
  *
  * The input are words from Kafka.
  *
  * This example shows how to:
  *  - write a simple Flink Streaming program,
  *  - use tuple data types,
  *  - write and use transformation functions.
  */
object WordCount {
  def main(args: Array[String]) {
    // set up the execution environment
    val env = StreamExecutionEnvironment
      .getExecutionEnvironment
      .setStateBackend(new FsStateBackend("file:///home/knoldus", true))

    // start a checkpoint every 1000 ms
    env.enableCheckpointing(1000)

    // set mode to exactly-once (this is the default)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    // make sure 500 ms of progress happen between checkpoints
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)

    // checkpoints have to complete within one minute, or are discarded
    env.getCheckpointConfig.setCheckpointTimeout(60000)

    // prevent the tasks from failing if an error happens in their checkpointing, the checkpoint will just be declined.
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)

    // allow only one checkpoint to be in progress at the same time
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    // prepare Kafka properties
    val kafkaProperties = new Properties
    kafkaProperties.setProperty("zookeeper.connect", "localhost:2181")
    kafkaProperties.setProperty("group.id", "flink")
    kafkaProperties.setProperty("bootstrap.servers", "localhost:9092")

    // set up Kafka Consumer
    val kafkaConsumer = new FlinkKafkaConsumer[String]("input", new SimpleStringSchema, kafkaProperties)

    println("Executing WordCount example.")

    // get text from Kafka
    val text = env.addSource(kafkaConsumer)

    val counts: DataStream[(String, Int)] = text
      // split up the lines in pairs (2-tuples) containing: (word,1)
      .flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      // group by the tuple field "0" and sum up tuple field "1"
      .keyBy(0)
      .mapWithState((in: (String, Int), count: Option[Int]) =>
        count match {
          case Some(c) => ((in._1, c), Some(c + in._2))
          case None => ((in._1, 1), Some(in._2 + 1))
        })

    // emit result
    println("Printing result to stdout.")
    counts.map(_.toString()).addSink(new FlinkKafkaProducer[String]("output", new SimpleStringSchema,
      kafkaProperties))

    // execute program
    env.execute("Streaming WordCount")
  }
}

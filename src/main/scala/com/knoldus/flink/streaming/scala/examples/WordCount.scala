package com.knoldus.flink.streaming.scala.examples

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

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
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // prepare Kafka consumer properties
    val kafkaConsumerProperties = new Properties
    kafkaConsumerProperties.setProperty("zookeeper.connect", "localhost:2181")
    kafkaConsumerProperties.setProperty("group.id", "flink")
    kafkaConsumerProperties.setProperty("bootstrap.servers", "localhost:9092")

    // set up Kafka Consumer
    val kafkaConsumer = new FlinkKafkaConsumer[String]("input", new SimpleStringSchema, kafkaConsumerProperties)

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
      .sum(1)

    // emit result
    println("Printing result to stdout.")
    counts.print()

    // execute program
    env.execute("Streaming WordCount")
  }
}

package com.mapr.example

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka09.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka09.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka09.LocationStrategies.PreferConsistent

object StreamingWithScala {
    var eventDStream : InputDStream[ConsumerRecord[String, Array[Byte]]] = _

    def main(args: Array[String]): Unit = {
        if (args.length < 4 ) {
            println("Usage: come.mapr.example.StreamingWithScala topic groupid batch_interval consume_rate")
            println("Example:")
            println("    come.mapr.example.StreamingWithScala /s1:topic abc 2 10")
            return
        }

        var topic   = args(0)
        var groupid = args(1)
        var interval= args(2).toLong
        var maxRate = args(3)
        var minRate = args(3)

        val sparkConf = new SparkConf().setAppName("StreamingWithScala")
            .set("spark.logConf", "true")
            .set("spark.streaming.backpressure.enabled", "true")
            .set("spark.streaming.kafka.maxRatePerPartition", maxRate)
            .set("spark.streaming.backpressure.pid.minRate", minRate)
            .set("spark.streaming.kafka.consumer.poll.ms", "1000")
        val ssc = new StreamingContext(sparkConf, Seconds(interval))

        val kafkaParams = Map[String, String](
            ConsumerConfig.GROUP_ID_CONFIG -> groupid,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer"
        )

        eventDStream = KafkaUtils.createDirectStream[String, Array[Byte]](
            ssc,
            PreferConsistent,
            Subscribe[String, Array[Byte]](Array(topic).toSet, kafkaParams)
        )

        eventDStream.foreachRDD { (rdd, time) =>
            val count = rdd.count
            println("Batch Time: " + time + ", Total: " + count)
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd.foreach(x => {
                    val y = offsetRanges.filter(o => o.partition == x.partition())(0)
                    println(time + "\t" + x.partition() + "\t" + x.offset() + "\t" + y.fromOffset + "-" + y.untilOffset )
            })
            eventDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        }
        ssc.start
        ssc.awaitTermination
    }
}


package com.sprint3

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ResuleSink{


  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("CurationService").master("local[*]").getOrCreate()

    val ssc=new StreamingContext(spark.sparkContext,Seconds(2));

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("curated_trip")
    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics,kafkaParams))

   stream.foreachRDD(rdd=> {
     val rddRow= rdd.map(record=> Row(record.value()))
     val df=spark.createDataFrame(rddRow,StructType(Seq(StructField("name",StringType))))
      df.write.text("/tmp/")
   })

    ssc.start()
    ssc.awaitTermination()
  }
}

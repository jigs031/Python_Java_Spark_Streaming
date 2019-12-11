package com.assignment.three

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Curator{

  def enricherSchema()  ={
    StructType(Seq(
        StructField("start_date", StringType, true),
        StructField("start_station", StringType, true),
        StructField("end_date", StringType, true),
        StructField("end_station", StringType, true),
        StructField("is_member", StringType, true),
        StructField("start__stations__station_id", StringType, true),
        StructField("start_data__stations__external_id", StringType, true),
        StructField("start_data__stations__name", StringType, true),
        StructField("start_data__stations__short_name", StringType, true),
        StructField("start_data__stations__lat", StringType, true),
        StructField("start_data__stations__lon", StringType, true),
        StructField("start_data__stations__rental_methods__001", StringType, true),
        StructField("start_data__stations__rental_methods__002", StringType, true),
        StructField("start_data__stations__capacity", StringType, true),
        StructField("start_data__stations__electric_bike_surcharge_waiver", StringType, true),
        StructField("start_data__stations__eightd_has_key_dispenser", StringType, true),
        StructField("end__stations__station_id", StringType, true),
        StructField("end_data__stations__external_id", StringType, true),
        StructField("end_data__stations__name", StringType, true),
        StructField("end_data__stations__short_name", StringType, true),
        StructField("end_data__stations__lat", StringType, true),
        StructField("end_data__stations__lon", StringType, true),
        StructField("end_data__stations__rental_methods__001", StringType, true),
        StructField("end_data__stations__rental_methods__002", StringType, true),
        StructField("end_data__stations__capacity", StringType, true),
        StructField("end_data__stations__electric_bike_surcharge_waiver", StringType, true),
        StructField("end_data__stations__eightd_has_key_dispenser", StringType, true)
    ))
  }
  def getStationStatus(spark:SparkSession) ={
    spark.read.json("/Users/jignesh/Desktop/Fiverr/David/sprint3/src/main/resources/station_status.json")
  }

  def Kafka(df:DataFrame): Unit ={

  }

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("CurationService").master("local[*]").getOrCreate()

    val stationStatus=getStationStatus(spark).as("stationStatus");
    stationStatus.show(10);
    stationStatus.printSchema()

    stationStatus.createOrReplaceTempView("status")

    val ssc=new StreamingContext(spark.sparkContext,Seconds(2));

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("enriched_trip")
    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics,kafkaParams))

   stream.foreachRDD(rdd=>
   {
        var rddRow=rdd.map(record=> {
          val message=record.value().split(",")
          System.out.println(message.length)
          Row(message(0), message(1), message(2), message(3), message(4), message(5), message(6),
            message(7), message(8), message(9), message(10), message(11), message(12), message(13), message(14), message(15), message(16), message(17), message(18), message(19), message(20), message(21), message(22), message(23),
            message(24), message(25), message(26))
        })
        var df=spark.createDataFrame(rddRow,enricherSchema()).as("stream")
        df.createOrReplaceTempView("streamSource");
        var resultDF= df.join(stationStatus, col("stream.end_station")=== col("stationStatus.station_id"),"inner")

        resultDF.printSchema();
        resultDF.show(10);
        resultDF.rdd.foreachPartition(partition =>
       partition.foreach {
         msg => {
           val props = new util.HashMap[String, Object]()

           props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
           props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
             "org.apache.kafka.common.serialization.StringSerializer")
           props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
             "org.apache.kafka.common.serialization.StringSerializer")

           val producer = new KafkaProducer[String, String](props)

           val csvMessage=msg.mkString(",");
           println(csvMessage)
           val message = new ProducerRecord[String, String]("curated_trip", null, csvMessage)
           producer.send(message)
           producer.flush();
         }
       }
      )
   });

    ssc.start()
    ssc.awaitTermination()
  }
}

package com.assignment.three

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object Curator{


  def getStationStatus(spark:SparkSession) ={
    spark.read.format("csv").option("header","true").load("/Users/jignesh/Desktop/Fiverr/David/Rushi's Friend/Curator/src/main/resources/status.csv")
  }

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("CurationService").master("local[*]").getOrCreate()
    import spark.implicits._

    val stationStatus=getStationStatus(spark).as("status");
    stationStatus.show(10);
    stationStatus.printSchema()


    val ssc=new StreamingContext(spark.sparkContext,Seconds(2));

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val enrichedTopic = Array("enriched_trip")
    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](enrichedTopic,kafkaParams))




    stream.foreachRDD(rdd=>
    {
      var rddRow=rdd.map(record=> {
       Parser.parse(record.value())
      })
      var df=spark.createDataFrame(rddRow,Parser.schema()).as("station")
      var resultDF= df.join(stationStatus, col("station.end_station")=== col("status.station_id"),"inner")

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

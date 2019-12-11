package com.assignment.three;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class EnrichStream {

    public static void main(String[] args){
        System.out.println("********* Starting the Enrich Stream *************");
        KafkaConsumer consumer= KafkaUtils.consumer();
        KafkaProducer producer= KafkaUtils.producer();
        Map<String,String[]> lookup=HDFSUtils.readCSVFile("hdfs://localhost:9000/user/data/station_information/station-info.csv");

        consumer.subscribe(Collections.singletonList("trip"));

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
            consumerRecords.forEach(record->{
                System.out.println("Record: "+ record.value());
                String[] message=record.value().split(",");
                String[] startStationInfo=lookup.get(message[1]);
                String[] endStationInfor=lookup.get(message[3]);
                String result=String.join(",",message)+","+ Strings.join( startStationInfo,",")+","+Strings.join(endStationInfor,",");
                producer.send(new ProducerRecord<String, String>("enriched_trip",message[1], result));
            });
        }

    }

}

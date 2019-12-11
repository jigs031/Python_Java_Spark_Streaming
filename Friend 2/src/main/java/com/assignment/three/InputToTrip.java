package com.assignment.three;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;


public class InputToTrip {

    /*
    Read the Content of File and Load into Kafka Line by line
     */
    public static void loadFileToKafka(String topic,String fileName){

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));

            KafkaProducer producer = KafkaUtils.producer();
            String csvLine = bufferedReader.readLine();
            while (csvLine != null) {
                System.out.println(csvLine);
                String key=csvLine.split(",")[1];
                producer.send(new ProducerRecord(topic,key, csvLine));
                csvLine = bufferedReader.readLine();
            }
            bufferedReader.close();
            producer.close();
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        String file="/Users/jignesh/Desktop/Fiverr/David/sprint3/src/main/resources/trip.csv";
        loadFileToKafka("trip",file);

    }
}

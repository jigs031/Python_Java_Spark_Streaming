package com.assignment.three;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;


public class File2Kafka {

    public static void main(String[] args) throws IOException {
        String file="/Users/jignesh/Desktop/Fiverr/David/Rushi's Friend/EnrichStream/src/main/resources/input.csv";
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));

            KafkaProducer producer = KafkaUtils.producer();
            String csvLine = br.readLine();
            while (csvLine != null) {
                System.out.println(csvLine);
                String key=csvLine.split(",")[1];
                producer.send(new ProducerRecord("trip",key, csvLine));
                csvLine = br.readLine();
            }
            br.close();
            producer.close();
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
    }
}

package com.kafka.assignment;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;
import java.util.Random;


public class Producer {
    public static void main(String[] args){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "com.kafka.assignment.UserSerializer");

        KafkaProducer kafkaProducer = new KafkaProducer(properties);
        try{
            for(int i = 1; i < 40; i++){

                Random ran = new Random();
                User user = new User(i, "Shreyash Kumar", ran.nextInt(80)+20, "Btech");
                System.out.println("Message " + user.toString() + " sent!");
                kafkaProducer.send(new ProducerRecord("user", Integer.toString(i), user));
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            kafkaProducer.close();
        }
    }
}
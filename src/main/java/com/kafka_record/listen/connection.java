package com.kafka_record.listen;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@SuppressWarnings("ALL")
public class connection {
    // init consumer kafka
    public Properties initProperties() {
        Properties info = new configs().info(); // thông tin cấu hình
        //Creating consumer properties
        Properties p = new Properties();
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, info.getProperty("BOOTSTRAP_SERVERS_CONFIG"));
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG, info.getProperty("GROUP_ID_CONFIG"));
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, info.getProperty("AUTO_OFFSET_RESET_CONFIG"));
        return p;
    }
}

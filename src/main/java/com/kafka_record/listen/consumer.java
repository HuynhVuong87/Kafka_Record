package com.kafka_record.listen;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;


@SuppressWarnings("ALL")
public class consumer {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(consumer.class.getName()); // khai báo log
        List<String> listWaitImport = new ArrayList<String>();
        //creating consumer
        KafkaConsumer<String,String> consumer= new KafkaConsumer<String, String>(new connection().initProperties()); // khởi tạo kết nối đến kafka

        Properties info_kafka = new configs().info();
        //Subscribing
        String topic = info_kafka.getProperty("TOPIC"); // Lấy tên topic

//        TopicPartition tp = new TopicPartition(topic, 0);
//        long offsetToReadFrom = 0;
//        List<TopicPartition> topics = Arrays.asList(tp);
//        consumer.assign(topics);
//        consumer.seek(tp, offsetToReadFrom);

        consumer.subscribe(Collections.singletonList(topic));

        //polling
        final boolean[] wait = {true, true};
        String count_length = info_kafka.getProperty("COUNT_LENGTH"); // lấy giới hạn record để lưu

        while(true){
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record: records){

                String value = record.value() + " - offset: " + record.offset();

                listWaitImport.add(value);
                // kiểm tra nếu record đạt đến giới hạn lưu quy định trong config
                if (listWaitImport.size() == Integer.parseInt(count_length)) {
                    SaveData(listWaitImport);
                    listWaitImport.clear();
                    wait[1]=false;
                    wait[0] = true;
                }

                // kiểm tra nếu sau thời gian đợi mà chưa đạt đủ giới hạn lưu thì lưu
                else if (wait[0]) {
                    wait[0] = false;
                    wait[1]= true;
                    Timer timer = new Timer();
                    timer.schedule((new TimerTask() {
                        @Override
                        public void run() {
                            if (wait[1]) {
                                SaveData(listWaitImport);
                                listWaitImport.clear();
                                wait[0] = true;
                            }

                        }
                    }), Integer.parseInt(info_kafka.getProperty("TIME_WAIT")));
                }
            }
        }


    }

    private static void SaveData(List data) {
        //test
        if (data.size() > 0) {
            Date date = new Date();
            //This method returns the time in millis
            long timeMilli = date.getTime();
            services s = new services();
            try {
                s.saveDataToFile( data, "testfile_" + timeMilli);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}

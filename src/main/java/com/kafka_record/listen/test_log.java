package com.kafka_record.listen;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.Properties;

@SuppressWarnings("ALL")
public class test_log {
    public static void main(String[] args) throws IOException, ParseException {

        // khai báo producer kafka
        Properties info_kafka = new configs().info();
        String topic = info_kafka.getProperty("TOPIC");
        KafkaProducer<String,String> first_producer = new KafkaProducer<String, String>(new connection().initProperties());

        String pathName = "C:\\Users\\huynh\\IdeaProjects\\kafka_record\\test"; // path đến thư mục chứa file log
        final File folder = new File(pathName);

        // đọc toàn bộ file trong thư mục khai báo trên
        for (final File fileEntry : Objects.requireNonNull(folder.listFiles())) {
            String file = pathName + "\\" + fileEntry.getName(); // get tên file

            String content;
            content = new String(Files.readAllBytes(Paths.get(file))); // get nội dung file

            String[] raws = content.split(";"); // tách các log theo dấu ;
            System.out.println(raws.length);

            // duyệt các raw log lấy được
            int i = 0;
            for (String raw: raws) {
                raw = raw.replace("\n", "").replace("\r", " ");
                if (raw.contains("*")) {
                    String[] items = raw.split("EST");
                    Date time;

                    // chuyển string sang date
                    SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yy HH:mm:ss");
                    String[] raw_time = items[0].trim().split("\\s");
                    time = formatter.parse(raw_time[1] + " " + raw_time[2]);

                    // khởi tạo class log
                    Log log = new Log();
                    log.time = time;
                    log.content = items[1].replaceAll("\\s+", " ").trim();

                    // Bắn vào topic kafka
                    i++;
                    String value = i + ". " + log.time + " - " + log.content + "\n";
                    ProducerRecord<String, String> record=new ProducerRecord<String, String>(topic, value);
                    first_producer.send(record);
                    System.out.println(value);
                    first_producer.flush();
                }
            }
        }
    }
}

package com.kafka_record.listen;

import java.util.*;

@SuppressWarnings("ALL")
public class configs {
    public Properties info() {
        Properties info_data = new Properties();
        info_data.put("BOOTSTRAP_SERVERS_CONFIG", "127.0.0.1:9092");
        info_data.put("AUTO_OFFSET_RESET_CONFIG", "latest");
        info_data.put("GROUP_ID_CONFIG", "test");
        info_data.put("TOPIC", "test_new_topic");
        info_data.put("COUNT_LENGTH", "400"); // giới hạn record để lưu
        info_data.put("TIME_WAIT", "5000"); // thời gian đợi để lưu
        return info_data;
    }
}

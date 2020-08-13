package com.kafka_record.listen;

import java.io.*;
import java.util.List;

public class services {
    public void saveDataToFile(List<String> data, String filename) throws IOException {

            System.out.println(data.size());
            FileWriter writer = new FileWriter(filename + ".txt", true);
            for (String item : data) {

                writer.write(item);
                writer.write("\n");   // write new line
            }
            writer.close();

    }
}

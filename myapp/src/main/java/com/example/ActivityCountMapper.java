package com.example;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;

public class ActivityCountMapper extends Mapper<Object, Text, Text, Text> {
    private static final Logger logger = LoggerFactory.getLogger(ActivityCountMapper.class);
    private Text userIds = new Text();
    private Text Flow = new Text();
    private boolean firstLine = true;
    private CSVParser csvParser;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line == null || line.isEmpty()) {
            return;
        }
        if (firstLine && line.contains("report_date")) {
            firstLine = false;
            return;
        }

        String[] columns = line.split(",");
        if (columns.length < 9) {
            return;
        }

        String userId = columns[0].trim();
        String reportedDate = columns[1].trim();
        String input = columns[5].trim();
        String output = columns[8].trim();
        if (input.isEmpty()) {
            input = "0";
        }
        if (output.isEmpty()) {
            output = "0";
        }

        String isActive = "0";
        if (!input.equals("0") || !output.equals("0")) {
            isActive = "1";
        }
        userIds.set(userId);
        Flow.set(reportedDate + "," + isActive);
        context.write(userIds, Flow);
    }
}

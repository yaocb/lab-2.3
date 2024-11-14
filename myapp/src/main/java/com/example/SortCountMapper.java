package com.example;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortCountMapper extends Mapper<Object, Text, DoubleWritable, Text> {
    private DoubleWritable activeNums = new DoubleWritable();
    private Text userIds = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String Lines = value.toString();
        String[] columns = Lines.split("\\t");
        if (columns.length != 2) {
            return;
        }

        String userId = columns[0];
        double activeNum = Double.parseDouble(columns[1]);

        activeNums.set(activeNum);
        userIds.set(userId);
        context.write(activeNums, userIds);
    }
}

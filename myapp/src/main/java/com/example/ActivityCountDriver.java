package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ActivityCountDriver {
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        conf.set("stopwords.path", args[1]);
        Job job1 = Job.getInstance(conf, "Word count");
        job1.setJarByClass(ActivityCountDriver.class);
        job1.setMapperClass(ActivityCountMapper.class);
        job1.setReducerClass(ActivityCountReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        if (!job1.waitForCompletion(true)) {
            System.exit(-1);
        }

        Job job2 = Job.getInstance(conf, "Sort");
        job2.setJarByClass(ActivityCountDriver.class);
        job2.setMapperClass(SortCountMapper.class);
        job2.setReducerClass(SortCountReducer.class);
        job2.setMapOutputKeyClass(DoubleWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setSortComparatorClass(DescendingDouble.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        if (!job2.waitForCompletion(true)) {
            System.exit(-1);
        }
    }
}

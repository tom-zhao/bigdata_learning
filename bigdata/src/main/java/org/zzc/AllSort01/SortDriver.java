package org.zzc.AllSort01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class SortDriver {


    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException {

        String in = args[0];
        String out = args[1];
        System.out.println(in + "  " + out + " ");

        // new Configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "zzc-all-sort");

        // worker class
        job.setJarByClass(SortDriver.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setPartitionerClass(SortPartitioner.class);

        // mapper output Type class
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        // reducer output type
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // in/out Paths..
        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        // waiting for MR tasks finished
        boolean flag = job.waitForCompletion(true);
        System.exit(flag?0:1);

    }
}

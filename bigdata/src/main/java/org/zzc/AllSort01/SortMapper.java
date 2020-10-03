package org.zzc.AllSort01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


// Mapping input  key, Value

public class SortMapper extends Mapper<LongWritable,Text, IntWritable, IntWritable> {
    IntWritable retKey = new IntWritable();
    IntWritable retVal = new IntWritable(1);
    public SortMapper() {
        super();
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Text Value -> IntWritable
        retKey.set(Integer.parseInt(value.toString().trim()));

        context.write(retKey, retVal);
    }
}

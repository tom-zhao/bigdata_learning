package org.zzc.wordcount;



import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] strings = StringUtils.split(value.toString(), " ");
        for (String s: strings) {
            context.write(new Text(s), new IntWritable(1));
        }
    }

}

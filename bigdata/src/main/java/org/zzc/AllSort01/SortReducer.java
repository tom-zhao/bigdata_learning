package org.zzc.AllSort01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// reduce
// in -> (int: null)
// out -> (int : int) order: key
public class SortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private static int order = 1;

    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable value: values) {
            context.write(new IntWritable(order), key);
            order++;
        }
    }
}

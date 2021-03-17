package org.zzc.AllSort01;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SortPartitioner  extends Partitioner<LongWritable, NullWritable> {

    public int getPartition(LongWritable key, NullWritable val, int i) {
        return 0;
    }
}

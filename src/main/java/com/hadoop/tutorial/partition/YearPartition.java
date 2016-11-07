package com.hadoop.tutorial.partition;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by baoyu on 16/11/6.
 */
public  class YearPartition extends Partitioner<IntWritable,Text> implements Configurable {

    private static final String  MIN_LAST_YEAR = "min.last.year";
    private Configuration conf = null;
    private int minLastYear = 0;  //(2008)


    @Override
    public void setConf(Configuration configuration) {
            this.conf = configuration;
            minLastYear = configuration.getInt(MIN_LAST_YEAR,0);

    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int getPartition(IntWritable key, Text text, int numPartitioner) {
        return key.get() - minLastYear;   //(2009 - 2008)
    }

    public static void setMinLastYear(Job job, int minLastYear){
        job.getConfiguration().setInt(MIN_LAST_YEAR,minLastYear);
    }
}

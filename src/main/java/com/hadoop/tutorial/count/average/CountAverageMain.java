package com.hadoop.tutorial.count.average;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by baoyu on 16/10/22.
 */
public class CountAverageMain {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CountAverage <input path> <output path>");
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(CountAverageMain.class);
        job.setJobName("CountAverage");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(AverageMapper.class);
        job.setReducerClass(AverageReducer.class);

        job.setOutputKeyClass(IntWritable.class);              //注1
        job.setOutputValueClass(CountAverageTuple.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

package com.hadoop.tutorial.media.standard;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * this show how to get standard deviation
 */
public class MedianStandardMain {

    public static void main(String[] args) throws Exception{
        if (args.length != 2) {
            System.err.println("Usage: MedianStandardMain <input path> <output path>");
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(MedianStandardMain.class);
        job.setJobName("MedianStandardMain");

        if(FileSystem.get(job.getConfiguration()).exists(new Path(args[1]))){
            FileSystem.get(job.getConfiguration()).delete(new Path(args[1]),true);
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MedianStandardMapper.class);
        job.setReducerClass(MedianStandardReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);


        job.setOutputKeyClass(IntWritable.class);              //注1
        job.setOutputValueClass(MedianStandardTuple.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);



    }
}

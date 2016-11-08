package com.hadoop.tutorial.multipleoutputs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * this show how to use MultipleOutputs
 * usage:
 *
 * input data:
 *          I love hadoop
 *          I love pig
 *          I love habse
 * output data:
 *          hbase-m-000000.txt
 *                      I love hbase
 *          pig-m-000000.txt
 *                      I love pig
 *          hadoop-m-000000.txt
 *                      I love hadoop
 */
public class MultipleoutputsMain {
          public static void main(String[] args) throws Exception{
            if (args.length != 2) {
                System.err.println("Usage: MultipleoutputsMain <input  path> <output path>");
                System.exit(-1);
            }
            Job job = new Job();
            job.setJarByClass(MultipleoutputsMain.class);
            job.setJobName("MultipleoutputsMain");

              FileInputFormat.addInputPath(job, new Path(args[0]));
              FileOutputFormat.setOutputPath(job, new Path(args[1]));

            if(FileSystem.get(job.getConfiguration()).exists(new Path(args[1]))){
                FileSystem.get(job.getConfiguration()).delete(new Path(args[1]),true);
            }
              job.setMapperClass(MultipleOutputMapper.class);


            MultipleOutputs.addNamedOutput(job,"multiple",TextOutputFormat.class,Text.class,NullWritable.class);
            MultipleOutputs.setCountersEnabled(job,true);
            job.setNumReduceTasks(0);
              job.setOutputKeyClass(Text.class);              //注1
            job.setOutputValueClass(NullWritable.class);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}

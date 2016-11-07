package com.hadoop.tutorial.multipleinputs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * newsId join
 */
public class MultipleInputsJoinMain  {
          public static void main(String[] args) throws Exception{
            if (args.length != 3) {
                System.err.println("Usage: MultipleInputsJoinMain <input one path> <input two path> <output path>");
                System.exit(-1);
            }
            Job job = new Job();
            job.setJarByClass(MultipleInputsJoinMain.class);
            job.setJobName("MultipleInputsJoinMain");

            if(FileSystem.get(job.getConfiguration()).exists(new Path(args[2]))){
                FileSystem.get(job.getConfiguration()).delete(new Path(args[2]),true);
            }

            MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class,NewsMapper.class);
            MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class,CommentsMapper.class);

            job.setReducerClass(NewsCommentsReducer.class);

            job.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job,new Path(args[2]));

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);              //注1
            job.setOutputValueClass(NullWritable.class);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}

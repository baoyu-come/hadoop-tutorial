package com.hadoop.tutorial.multipleinputs;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by baoyu on 16/11/6.
 */
public class NewsCommentsReducer extends Reducer<Text,Text,Text,NullWritable> {
    private ArrayList<String> comments = new ArrayList<String>();
    private String news = null;
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                comments.clear();;
                news = null;
                for (Text t:values) {
                     if(t.toString().startsWith("News")){
                         news = t.toString().substring(4);
                     }else{
                         comments.add(t.toString().substring(3));
                     }
                }
                if (news!=null){
                    StringBuilder sb = new StringBuilder();
                    sb.append(news);
                    for (String comment:comments) {
                        sb.append("#");
                        sb.append(comment);
                    }
                    context.write(new Text(sb.toString()),NullWritable.get());
                }
    }
}

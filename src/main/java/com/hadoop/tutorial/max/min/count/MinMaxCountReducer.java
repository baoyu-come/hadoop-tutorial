package com.hadoop.tutorial.max.min.count;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by baoyu on 16/10/22.
 */
public class MinMaxCountReducer extends Reducer<Text,MinMaxCountTuple,Text,MinMaxCountTuple>{
    private MinMaxCountTuple result = new MinMaxCountTuple();

    @Override
    protected void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context) throws IOException, InterruptedException {
        result.setCount(0);
        result.setMax(null);
        result.setMin(null);
        int sum = 0;
        for (MinMaxCountTuple val:values) {
            if(result.getMin()==null||val.getMin().compareTo(result.getMin()) < 0){
                result.setMin(val.getMin());
            }
            if(result.getMax()==null||val.getMax().compareTo(result.getMax()) > 0){
                result.setMax(val.getMax());
            }
            sum += val.getCount();
        }
        result.setCount(sum);
        context.write(key,result);
    }
}

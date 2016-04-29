package com.kali.bigram;

/**
 * Created by kalit_000 on 26/04/2016.
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class BigramReducer extends Reducer<TextPair,IntWritable,Text,IntWritable>{

    @Override
    public void reduce(TextPair key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException {

        int count=0;
        for (IntWritable value: values)
        {
            count +=value.get();
        }

        context.write(new Text(key.toString()),new IntWritable(count));
    }




}

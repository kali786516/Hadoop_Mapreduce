package com.kali.mapreduce;

/**
 * Created by kalit_000 on 23/04/2016.
 */

import org.apache.commons.math.ode.IntegratorException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.io.InterruptedIOException;


public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    //map hadoop map method
    @Override
    public void map(LongWritable Key,Text Value,Context context) throws IOException,InterruptedException {

        String Line=Value.toString();

        for(String word:Line.split(" ")){
            if(word.length() > 0)
            {
                context.write(new Text(word),new IntWritable(1));
            }
        }


    }



}

package com.kali.bigram;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;


/**
 * Created by kalit_000 on 25/04/2016.
 */


public class BigramMap extends Mapper<LongWritable,Text,TextPair,IntWritable> {

    //op:-word,filename  (where filename is nothing but website name)

    private Text lastword =null;
    private Text currentword = new Text();

    @Override

    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String line = value.toString();//get lines from file
        line=line.replace(",","");//clean up
        line=line.replace(".","");//clean up


        for(String word:line.split(" ")){

            if (lastword == null)  // if lastword is null dont write anything
            {
                lastword=new Text(word);
            }
            else
            {
                currentword.set(word);
                context.write(new TextPair(lastword,currentword),new IntWritable(1));//key is combination of last and current word and value is 1
                lastword.set(currentword.toString());
            }

        }


    }
}

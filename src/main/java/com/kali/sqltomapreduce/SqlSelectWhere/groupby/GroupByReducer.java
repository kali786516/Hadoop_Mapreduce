package com.kali.sqltomapreduce.SqlSelectWhere.groupby;

/**
 * Created by kalit_000 on 27/04/2016.
 */

import org.apache.commons.collections.IterableMap;
import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobHistory;
import org.apache.hadoop.mapreduce.Reducer;
import org.jboss.netty.handler.codec.http.HttpHeaders;

import java.io.IOException;

public class GroupByReducer extends Reducer<Text,DoubleWritable,Text,Text> {

    @Override
    public void reduce(final Text key,final Iterable<DoubleWritable> values,final Context context) throws IOException,InterruptedException {

        Double maxvalue=Double.MIN_VALUE;
        Double minvalue=Double.MIN_VALUE;
        String op=null;

        for (DoubleWritable value: values)
        {
            maxvalue=Math.max(maxvalue,value.get());
            minvalue=Math.min(minvalue,value.get());
            op=maxvalue.toString()+" "+minvalue.toString();
        };

        if (maxvalue >  20)
        {
            context.write(key,new Text(op));
        }



    }


}

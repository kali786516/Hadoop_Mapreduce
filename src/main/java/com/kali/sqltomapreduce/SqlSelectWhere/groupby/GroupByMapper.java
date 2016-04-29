package com.kali.sqltomapreduce.SqlSelectWhere.groupby;

/**
 * Created by kalit_000 on 27/04/2016.
 */

import org.apache.commons.math.ode.IntegratorException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.io.InterruptedIOException;

/*
* select symbol,max(openprice)
 * where series="EQ" groupbysymbol having max(open) > 20
*
*
* */


public class GroupByMapper extends Mapper<LongWritable,Text,Text,DoubleWritable> {

    @Override
    public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
    {
        String Line=value.toString();
        String[] Data=Line.split(",");

        if (Data[0].equals("Symbol"))
        {
            return;
        }

        else
        {
            if(Data[1].equals("equals"))
            {
                String symbol=Data[0]; // symbol column
                Double openprice=Double.parseDouble(Data[2]); //openprice

                context.write(new Text(symbol),new DoubleWritable(openprice));


            }
        }

    }



}

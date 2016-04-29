package com.kali.sqltomapreduce.SqlSelectWhere.sqlselect;

/**
 * Created by kalit_000 on 27/04/2016.
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/* SQL Conversion:-
* select symbol,open,close,timestamp from table
* where symbol=3MINDIA
* and series=EQ
* */


public class SqlMapper extends Mapper<LongWritable,Text,LongWritable,Text>  {

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
            if(Data[0].equals("3MINDIA") && Data[1].equals("equals"))
            {
                String output=Data[0]+" "+Data[2]+" "+Data[5]+" "+Data[10];

                context.write(key,new Text(output));


            }
        }

    }




}

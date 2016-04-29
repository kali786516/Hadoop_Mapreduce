package com.kali.invertedindex;

/**
 * Created by kalit_000 on 25/04/2016.
 */



import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/*
* input data:-
* URL,contents
* abc.com,this is a hadoop website
* bbc.com,this is a hadoop website
*
* MR OP Data:-
* word,URL LIST
* website,(abc.con,def.com)
* this,(abc.com,def.com)
*
* Hive:-
* http://stackoverflow.com/questions/6445339/collect-set-in-hive-keep-duplicates
*
* select word,collect_set(URL) from wordcountdataset
*
* Spark:-GroupByKey
*
* Bigram:-
* val filerdd=sc.textfile(file)
* val table=filerdd.split(",").map(x => (x(1),x(0))) // websitename,text
*
* val wordcounts=table.flatMap{ case(website,text) => for(word <- text.split(" ")) yield ((website,word),1))}
*                      .reduceByKey((x,y) => x+y)
*
* val op=wordcounts.map{ case((website,word),count) => (website,(word,count))}
*                  .groupByKey()
*
* Inverted Index Scala:-
* val filerdd=sc.textfile(file)
* val table=filerdd.split(",").map(x => (x(1),x(0))) // text,website
*
* table.flatMap{ case(text,website) => for (word <- text.split(" ")) yield (website,word)}
*      .map{case (text,website) => (text,website)}.groupByKey()
*
* */


public class InvertedIndexMapper extends Mapper<LongWritable,Text,Text,Text> {

    //op:-word,filename  (where filename is nothing but website name)

    private Text word=new Text(); //store the map output written to context
    private Text filename=new Text();// store the map output written to context

    @Override

    public void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException {

        /*will get us filename for the current line thats being processed*/

        FileSplit currentSplit=((FileSplit) context.getInputSplit()); // context.getInputsplit gets input split the mapper is currently proccessing
        // file split is a subclass of input split
        String filenamestr=currentSplit.getPath().getName();//gets  filename of file
        filename=new Text(filenamestr); //file nam stored in a variable

        String line= value.toString();//get lines from file
        StringTokenizer tokenizer=new StringTokenizer(line);// get words from file based on word similar to flatmap

        while(tokenizer.hasMoreTokens()){
            word.set(tokenizer.nextToken());
            context.write(word,filename);//word,filename
        }
    }
}

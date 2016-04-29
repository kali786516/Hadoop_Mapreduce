package com.kali.invertedindex;

/**
 * Created by kalit_000 on 25/04/2016.
 */


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

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

* */

/*
* Reduce Class Input:-
* Reduce ClassInp:-
* this,abc.com
* this,xyz.com
* is,abc.com
* is,xyz.com
* kali,abc.com
* kali,xyz.com
*
* Reduce ClassOutput:-  Spark GroupByKey
* this,abc.com|xyz.com
* kali,abc.com|xyx.com
* is,abc.com|xyz.com
*
*
* */


public class InvertedIndexReducer extends Reducer<Text,Text,Text,Text> {

   @Override
    public void reduce(final Text key,final Iterable<Text> values,final Context context) throws IOException,InterruptedException {

       StringBuilder stringBuilder=new StringBuilder();//used to concatenate all the filenames (website names) seperated by delimeter (|)

       for(Text value:values) {
           stringBuilder.append(value.toString());//iterate list of filenames (website) based on key and add to string builder

       if (values.iterator().hasNext()) {
           stringBuilder.append("|");// seperate each file name (website name) based on key with pipe as delimeter
       }

       }

       context.write(key,new Text(stringBuilder.toString()));// op to context class

   }

}

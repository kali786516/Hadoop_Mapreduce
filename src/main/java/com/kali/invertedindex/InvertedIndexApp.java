package com.kali.invertedindex;

/**
 * Created by kalit_000 on 25/04/2016.
 */
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.SystemClock;

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


public class InvertedIndexApp extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        if (args.length != 2)
        {
            System.err.println("Usage: WorcCount <file input path> <output path>");
        }


                 Job job=Job.getInstance(getConf());
                 job.setJobName("InvertedIndexApp");
                 job.setJarByClass(InvertedIndexApp.class);

                 job.getConfiguration().set("mapreduce.output.textoutputformat.seperator","|");

                // Specify key / value
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                // Setup MapReduce job
                // Do not specify the number of Reducer
                job.setMapperClass(InvertedIndexMapper.class);
                job.setReducerClass(InvertedIndexReducer.class);

                Path inputFilePath=new Path(args[0]);
                Path outputFilePath=new Path(args[1]);
                FileInputFormat.addInputPath(job,inputFilePath);
                FileOutputFormat.setOutputPath(job,outputFilePath);

                //FileInputFormat.set(job.true);


                //FileInputFormat.setInputDirRecursive(job,true);


                // Execute job and return status
                return job.waitForCompletion(true) ? 0 : 1;
            }

         public static void main(String[] args ) throws Exception{
             int exitCode=ToolRunner.run(new InvertedIndexApp(),args);
             System.exit(exitCode);
        }
    }




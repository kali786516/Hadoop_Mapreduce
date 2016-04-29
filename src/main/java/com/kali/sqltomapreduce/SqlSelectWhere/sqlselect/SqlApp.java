package com.kali.sqltomapreduce.SqlSelectWhere.sqlselect;

/**
 * Created by kalit_000 on 27/04/2016.
 */
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class SqlApp extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        if (args.length != 2)
        {
            System.err.println("Usage: WorcCount <file input path> <output path>");
        }


        Job job=Job.getInstance(getConf());
        job.setJobName("SqlApp");
        job.setJarByClass(SqlApp.class);

        job.getConfiguration().set("mapreduce.output.textoutputformat.seperator","|");

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Setup MapReduce job
        // Do not specify the number of Reducer
        job.setMapperClass(SqlMapper.class);
        job.setReducerClass(SqlReducer.class);

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
        int exitCode= ToolRunner.run(new SqlApp(),args);
        System.exit(exitCode);
    }
}

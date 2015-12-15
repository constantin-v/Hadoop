/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ema.hadoop.bestclient;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.ema.hadoop.bestclient.BCMapper;
import com.ema.hadoop.bestclient.BCReducer;
import org.apache.hadoop.mapred.JobConf;


/**
 *
 * @author student
 */
public class BestClient {
    public static void main(String[] args) throws Exception {
        
        if (args.length != 4) {
            System.err.println("Usage: BestClient <input path> <output path> <date start> <date end>");
            System.exit(-1);
        }
        
        Job job = Job.getInstance();
        job.setJarByClass(BestClient.class);
        job.setJobName("Best client job");        
        
        JobConf jobConf = (JobConf) job.getConfiguration();
        jobConf.setStrings("dates",args[2],args[3]);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(BCMapper.class);
        job.setReducerClass(BCReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

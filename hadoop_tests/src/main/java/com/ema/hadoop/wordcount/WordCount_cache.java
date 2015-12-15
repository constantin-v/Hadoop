package com.ema.hadoop.wordcount;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import static java.lang.System.out;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 *
 * @author sharispe
 */
public class WordCount_cache {
    
    public static void main(String[] args) throws Exception {
        
        if (args.length != 2) {
            System.err.println("Usage: WordCount <input path> <output path>");
            System.exit(-1);
        }
        
        
        // First we write the stop word list
        // it could also be a file manually loaded into HDFS
        
        String[] stopwords = {"the","a"};
        Configuration configuration = new Configuration();
        FileSystem hdfs = FileSystem.get( new URI( "hdfs://localhost:9000" ), configuration );
        Path file = new Path("hdfs://localhost:9000/user/student/stop_words.txt");
        if ( hdfs.exists( file )) { hdfs.delete( file, true ); } 
        OutputStream os = hdfs.create( file,
            new Progressable() {
                @Override
                public void progress() {
                    out.println("...bytes written");
                } });
        BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
        for(String w : stopwords){
            br.write(w+"\n");
        }
        
        br.close();
        hdfs.close();
        
        Job job = Job.getInstance();
        job.addCacheFile(new Path("hdfs://localhost:9000/user/student/stop_words.txt").toUri());
        
        job.setJarByClass(WordCount_cache.class);
        job.setJobName("Word count job");
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(WCMapper_cache.class);
        job.setReducerClass(WCReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

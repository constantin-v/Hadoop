package com.ema.hadoop.wordcount;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author sharispe
 */
public class WCMapper_cache extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    Set<String> stopwords;
    @Override
    protected void setup(
            Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        
        
        if (context.getCacheFiles() != null
                && context.getCacheFiles().length > 0) {
            
            stopwords = new HashSet();

            File stopword_file = new File("./stop_words.txt");
            
            try (BufferedReader br = new BufferedReader(new FileReader(stopword_file))) {
                String line;
                while ((line = br.readLine()) != null) {
                   stopwords.add(line.trim());
                }
            }
            
        }
        super.setup(context);
    }
    
    @Override
    protected void cleanup(
            Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        if (context.getCacheFiles() != null
                && context.getCacheFiles().length > 0) {

            File stopword_file = new File("./stop_words.txt");
            stopword_file.delete();
        }
        super.setup(context);
    }

    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {
            
            String nextToken = tokenizer.nextToken();
            if(!stopwords.contains(nextToken)){
                context.write(new Text(nextToken), new IntWritable(1));
            }
        }
    }
}
    

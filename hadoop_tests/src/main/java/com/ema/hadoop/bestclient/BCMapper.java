/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ema.hadoop.bestclient;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
        
        try{  
            String line = value.toString();
            String[] lineTab = line.split(";");

            String client = lineTab[0];
            int somme = Integer.parseInt(lineTab[2]);
            Date date = formatter.parse(lineTab[1]);

            JobConf jobConf = (JobConf) context.getConfiguration();
            String[] dateTable = jobConf.getStrings("dates");           

            Date dateStart = formatter.parse(dateTable[0]);
            Date dateEnd = formatter.parse(dateTable[1]);
            
            if(date.after(dateStart) && date.before(dateEnd)){
                context.write(new Text(client), new IntWritable(somme)); 
            } else {
                Logger.getLogger(BCMapper.class.getName()).log( Level.INFO, "ELSE ddddddddddddddddddddd");
                Logger.getLogger(BCMapper.class.getName()).log( Level.INFO,"param start " + dateTable[0]);                
                Logger.getLogger(BCMapper.class.getName()).log( Level.INFO,"Date start " + dateStart.toString());
                Logger.getLogger(BCMapper.class.getName()).log( Level.INFO,"param fin " + dateTable[1]);
                Logger.getLogger(BCMapper.class.getName()).log( Level.INFO,"Date fin " + dateEnd.toString());
            }
            
        } catch (ParseException e){
             Logger.getLogger(BCMapper.class.getName()).log( Level.INFO, "Parse exception");
        }
                        
              
    }
}

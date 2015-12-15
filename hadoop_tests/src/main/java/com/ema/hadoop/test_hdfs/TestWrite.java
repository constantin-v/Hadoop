/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ema.hadoop.test_hdfs;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import static java.lang.System.out;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

/**
 *
 * @author student
 */
public class TestWrite {
    
    public static void main(String[] args) throws IOException, URISyntaxException{
        
        Configuration configuration = new Configuration();
        FileSystem hdfs = FileSystem.get( new URI( "hdfs://localhost:9000" ), configuration );
        Path file = new Path("hdfs://localhost:9000/user/student/text_file_write.txt");
        if ( hdfs.exists( file )) { hdfs.delete( file, true ); } 
        OutputStream os = hdfs.create( file,
            new Progressable() {
                @Override
                public void progress() {
                    out.println("...bytes written");
                } });
        BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
        br.write("This is just a test to check if it is possible to write a file on HDFS using the Java API");
        br.close();
        hdfs.close();
        
        
    }
    
}

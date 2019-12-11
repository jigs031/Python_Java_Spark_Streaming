package com.assignment.three;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class HDFSUtils {

    public static Map<String,String[]> readCSVFile(String file)  {
        System.out.println("**** Reading Lookup file: "+file);
        Map<String,String[]> map=new HashMap<String, String[]>();
        Configuration conf=new Configuration();
        FileSystem fs = null;

        InputStream in = null;
        try {
            fs = FileSystem.get(URI.create(file), conf);
            in = fs.open(new Path(file));
            BufferedReader br=new BufferedReader(new InputStreamReader(in));
            String csvLine=br.readLine(); //Skip Header
            csvLine=br.readLine();
            while(csvLine!=null){
                map.put(csvLine.split(",")[0],csvLine.split(","));
                csvLine=br.readLine();
            }
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        finally {
            IOUtils.closeStream(in);
        }
        return map;
    }
}

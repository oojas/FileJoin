package com.fj.hadoop;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.*;
public class fjReducer extends Reducer<Text, JoinWritable, NullWritable, Text>{
    public void reduce(Text key, Iterable<JoinWritable> values, Context context)throws IOException,InterruptedException
    {
    	String name="";
    	String dept="";
    	for(JoinWritable wow:values)
    	{
    		if(wow.getMrFileName().toString().equals("empname.txt"))
    		{
    			name=wow.getMrValue().toString();
    		}
    		else if(wow.getMrFileName().toString().equals("empdept.txt"))
    		{
    			dept=wow.getMrValue().toString();
    		}
    	}
    	StringBuilder rec=new StringBuilder(key.toString()).append(",");
    	rec.append(name).append(",").append(dept);
    	context.write(NullWritable.get(),new Text(rec.toString()));
    }
}

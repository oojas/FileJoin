package com.fj.hadoop;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

import org.apache.hadoop.io.*;

public class fjMappper extends Mapper<LongWritable, Text, Text,JoinWritable> {
	String InputFileName;
	@Override
	protected void setup(Context context) throws IOException,InterruptedException
	{
	     FileSplit filesplit=(FileSplit) context.getInputSplit();	
	     InputFileName=filesplit.getPath().getName();
	}
    public void map(LongWritable key, Text value, Context context)throws IOException,InterruptedException
    {
    	String s[]=value.toString().split(",");
    	if(s.length==2)
    	{
    		context.write(new Text(s[0]),new JoinWritable(s[1],InputFileName));
    	}
    }
}

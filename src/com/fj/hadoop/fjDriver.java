package com.fj.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class fjDriver {
	public static void main(String[] args) throws IOException,InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub

		Configuration conf=new Configuration();
		String otherargs[]=new GenericOptionsParser(conf,args).getRemainingArgs();
		if(otherargs.length!=2)
		{
			System.err.print("Not enough arguments");
			System.exit(2);
		}
		Job job =new Job(conf,"File Join");
		job.setJarByClass(fjDriver.class);
		job.setMapperClass(fjMappper.class);
		job.setReducerClass(fjReducer.class);
		//job.setCombinerClass(sarReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinWritable.class);
		for(int i=0 ; i<otherargs.length-1 ; i++)
		{
			FileInputFormat.addInputPath(job,new Path(otherargs[i]));
		}
		FileOutputFormat.setOutputPath(job,new Path(otherargs[otherargs.length-1]));
		System.exit(job.waitForCompletion(true)?0 :1);
	}

}

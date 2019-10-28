package edu.gatech.cse6242;

import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.Object;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Q4 {

    public static class DifferenceMapper
    	extends Mapper<Object, Text, IntWritable, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable oneMinus = new IntWritable(-1);
    //private IntWritable node = new IntWritable();
    private IntWritable outDegree = new IntWritable();
    private IntWritable inDegree = new IntWritable();
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	 StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
    	 while (itr.hasMoreTokens()){
    	 	String line = itr.nextToken();
    	 	String tokens[] = line.split("\t");
    		outDegree.set(Integer.parseInt(tokens[0]));
    		context.write(outDegree, one);
    		inDegree.set(Integer.parseInt(tokens[1]));
    		context.write(inDegree, oneMinus);

    		}
    	}
	}
	public static class DifferenceReducer
       extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      	int sum = 0;
      	for (IntWritable val : values) {
        	sum += val.get();
      	}
      	result.set(sum);
      	context.write(key, result);
    	}
  	}

  	public static class CountMapper
    extends Mapper<Object, Text, IntWritable, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private IntWritable difference = new IntWritable();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
    	while (itr.hasMoreTokens()) {
    		String line = itr.nextToken();
        	String tokens[] = line.split("\t");

        	difference.set(Integer.parseInt(tokens[1]));
        	context.write(difference, one);
      		}
    	}
  	}

  	public static class CountReducer
       	extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      	int sum = 0;
      	for (IntWritable val : values) {
        	sum += val.get();
      	}
      	result.set(sum);
      	context.write(key, result);
    	}
  	}

  	public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	Job job1 = Job.getInstance(conf, "Q4");
    	/* TODO: Needs to be implemented */
    	job1.setJarByClass(Q4.class);
    	job1.setMapperClass(DifferenceMapper.class);
    	job1.setCombinerClass(DifferenceReducer.class);
    	job1.setReducerClass(DifferenceReducer.class);
    	job1.setOutputKeyClass(IntWritable.class);
    	job1.setOutputValueClass(IntWritable.class);
    	FileInputFormat.addInputPath(job1, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job1, new Path("job1_output"));
    	job1.waitForCompletion(true);

    	//Job2
    	Job job2 = Job.getInstance(conf, "Q4_res");
		job2.setJarByClass(Q4.class);
    	job2.setMapperClass(CountMapper.class);
    	job2.setCombinerClass(CountReducer.class);
    	job2.setReducerClass(CountReducer.class);
    	job2.setOutputKeyClass(IntWritable.class);
    	job2.setOutputValueClass(IntWritable.class);
    	FileInputFormat.addInputPath(job2, new Path("job1_output"));
    	FileOutputFormat.setOutputPath(job2, new Path(args[1]));
  	  	System.exit(job2.waitForCompletion(true) ? 0 : 1);
  	}
}
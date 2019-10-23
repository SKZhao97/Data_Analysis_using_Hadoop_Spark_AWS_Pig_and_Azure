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

public class Q1 {


  public static class TokenizerMapper
       extends Mapper<Object, Text, IntWritable, Text>{

    private IntWritable src = new IntWritable();
    private Text tgt = new Text();
    private Text weight = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
      while (itr.hasMoreTokens()) {
        String line = itr.nextToken();
        String tokens[] = line.split("\t");

	src.set(Integer.parseInt(tokens[0]));
        tgt.set(tokens[1]);
        weight.set(tokens[2]);
        context.write(src, new Text(tgt+","+weight));
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<IntWritable,Text, IntWritable,Text> {

    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int max = -1;
      int targetMax=0;
/*
      for (IntWritable val : values) {
        if(val.get() > max) max = val.get();
      }
      result.set(max);
      context.write(key, Text result);
    }
*/
      for (Text value: values){
	String compositeString = value.toString();
	String compositeStringArray[] = compositeString.split(",");
	int tempTarget = Integer.parseInt(compositeStringArray[0]);
	int tempWeight = Integer.parseInt(compositeStringArray[1]);
	if(tempWeight>max){
	    max=tempWeight;
	    targetMax=tempTarget;
	  }
	if(tempWeight==max){
	    targetMax=(tempTarget-targetMax)<0?tempTarget:targetMax;
	  }
	}
	if (max!=-1){
	Text keyText = new Text(targetMax+","+max);
	context.write(key, keyText);
	}

  }
}


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Q1");

    /* TODO: Needs to be implemented */
    job.setJarByClass(Q1.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

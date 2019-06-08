package org.usp.hadoopdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.usp.hadoopdemo.model.wrapper.WordCountMapper;
import org.usp.hadoopdemo.model.wrapper.WordCountReducer;

import java.io.IOException;
import java.nio.file.Path;

//@SpringBootApplication
public class HadoopdemoApplication {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//SpringApplication.run(HadoopdemoApplication.class, args);



		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(HadoopdemoApplication.class);
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path((args[0])));

		FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path((args[1])));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}

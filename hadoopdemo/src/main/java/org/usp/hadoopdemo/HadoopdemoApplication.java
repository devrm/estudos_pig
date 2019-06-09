package org.usp.hadoopdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.usp.hadoopdemo.model.wrapper.SalesMapper;
import org.usp.hadoopdemo.model.wrapper.SalesReducer;
import org.usp.hadoopdemo.model.wrapper.WordCountMapper;
import org.usp.hadoopdemo.model.wrapper.WordCountReducer;

import java.io.IOException;
import java.nio.file.Path;

//@SpringBootApplication
public class HadoopdemoApplication {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		Job job = null;
		if (args[2].equals("wc")) {
			job = Job.getInstance(conf, "word count");
			job.setMapperClass(WordCountMapper.class);
			job.setCombinerClass(WordCountReducer.class);
			job.setReducerClass(WordCountReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

		} else {
			job = Job.getInstance(conf, "sales");
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			// Specify names of Mapper and Reducer Class
			job.setMapperClass(SalesMapper.class);
			job.setReducerClass(SalesReducer.class);

			// Specify formats of the data type of Input and output
			//job.setInputFormat(TextInputFormat.class);
			//job.setOutputFormat(TextOutputFormat.class);
		}

		job.setJarByClass(HadoopdemoApplication.class);

		FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path((args[0])));
		FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path((args[1])));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}

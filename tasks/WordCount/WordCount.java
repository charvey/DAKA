
import daka.compute.CountReduce;
import daka.core.Task;
import daka.core.TaskConfig;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount extends Task {
	@Override
	public void PreExecute(TaskConfig config){}

	@Override
	public void Execute(TaskConfig config){
		try{

			Configuration conf = new Configuration();
conf.set("fs.default.name", "hdfs://localhost:9000");
conf.set("mapred.job.tracker", "localhost:9001");

			Job job = new Job(conf, "wordcount");

			job.setJarByClass(WordCount.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			job.setMapperClass(WordMapper.Map.class);
			job.setReducerClass(CountReduce.Reduce.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(config.getInputPath()));
			FileOutputFormat.setOutputPath(job, new Path(config.getOutputPath()));

			job.waitForCompletion(true);
		} catch(IOException ex){
		} catch(InterruptedException ex){
		} catch(ClassNotFoundException ex){
		} catch(Exception ex){
		}
	}

	@Override
	public void PostExecute(TaskConfig config){}
}

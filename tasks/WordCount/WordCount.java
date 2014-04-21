
import daka.compute.CountReduce;
import daka.core.Task;
import daka.core.TaskConfig;
import daka.io.FileInput;
import daka.io.FileOutput;

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
	public void PreExecute(TaskConfig config){
		String inputFile=config.getArguments().get("if");
		if(inputFile!=null)
		{
			FileInput fi=new FileInput(inputFile,
				config.getInputPath(),config.getConfig());

			try{
				fi.Update();
			} catch(IOException ex){
			}
		}
	}

	@Override
	public void Execute(TaskConfig config){
		try{
			Job job = new Job(config.getConfig(), "wordcount");

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
	public void PostExecute(TaskConfig config){
		String outputFile=config.getArguments().get("of");
		if(outputFile!=null)
		{
			FileOutput fo=new FileOutput(
				config.getOutputPath()+"/part-r-00000",
				outputFile,config.getConfig());

			try{
				fo.Update();
			} catch(IOException ex){
			}
		}
	}
}

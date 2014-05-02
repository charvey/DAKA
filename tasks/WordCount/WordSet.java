
import daka.core.Task;
import daka.core.TaskConfig;
import daka.io.FileInput;
import daka.io.FileOutput;
import daka.util.FileUtil;

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

public class WordSet extends Task {
	@Override
	public void PreExecute(TaskConfig config){
		String inputFile=config.getArguments().get("if");
		if(inputFile!=null)
		{
			FileInput fi=new FileInput(inputFile,
				config.getInputPath(),config.getConfig());

			try{
				fi.Update();

				FileUtil.delete(config.getConfig(), new Path(config.getOutputPath()));
			} catch(IOException ex){
			}
		}
	}

	@Override
	public void Execute(TaskConfig config){
		try{
			Job job = new Job(config.getConfig(), "wordset");

			job.setJarByClass(WordCount.class);

			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);

			job.setMapperClass(WordRepeater.Map.class);
			job.setReducerClass(WordJoiner.Reduce.class);

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

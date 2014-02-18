import daka.compute.CountReduce;
import daka.core.Task;
import daka.core.TaskConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CountAll extends Task {
	@Override
	public void PreExecute(TaskConfig config){
		//TODO setup input file 
		//FileInput input=new FileInput();
	}

	@Override
	public void Execute(TaskConfig config){
		try{
			Configuration conf = new Configuration();
			Job job = new Job(conf, "countall");

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			job.setMapperClass(CSVFieldMapper.Map.class);
			job.setCombinerClass(CountReduce.Reduce.class);
			job.setReducerClass(CountReduce.Reduce.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(config.getInputPath()));
			FileOutputFormat.setOutputPath(job, new Path(config.getOutputPath()));

			job.waitForCompletion(true);
		} catch(Exception ex){

		}
	}

	@Override
	public void PostExecute(TaskConfig config){
		//TODO Output to a file
	}
}

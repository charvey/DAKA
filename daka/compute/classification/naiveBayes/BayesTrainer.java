package daka.compute.classification.naiveBayes;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;

public class BayesTrainer 
{
	public static void  main(String[] args) throws Exception
	{
		String input = args[0];
		String output = args[1];
		
		runJob(input, output);
	}
	
	public static void runJob(String input, String output) throws Exception
	{
		JobConf conf = new JobConf(BayesTrainer.class);
		conf.setJobName("BayesTrainerjob");

		conf.setMapOutputKeyClass(Person.class);
		conf.setMapOutputValueClass(LongWritable.class);

		conf.setMapperClass(BayesMapper.class);
		conf.setCombinerClass(BayesReducer.class);
		conf.setReducerClass(BayesReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

	    FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		JobClient.runJob(conf);
	}
}

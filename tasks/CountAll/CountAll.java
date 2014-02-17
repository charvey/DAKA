import daka.compute.CountReduce;

public class CountAll extends Task{
	@Override
	public void PreExecute(){
		//TODO setup input file 
		//FileInput input=new FileInput();
	}

	@Override
	public void Execute(){
		JobConf conf = new JobConf(CountAll.class);
		conf.setJobName("pipcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(CSVFieldMapper.class);
		conf.setCombinerClass(CountReduce.class);
		conf.setReducerClass(CountReduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

	    FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}

	@Override
	public void PostExecute(){
		//TODO Output to a file
	}
}
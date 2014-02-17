import daka.compute.CountReduce;

public class CountPeople extends Task {
	@Override
	public void PreExecute(){
		//TODO setup input file 
		//FileInput input=new FileInput();
	}

	@Override
	public void Execute(){
		JobConf conf = new JobConf(CountPeople.class);
		conf.setJobName("pipcount");

		conf.setOutputKeyClass(Person.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(PeopleMapper.class);
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
public class WordCount extends Task {
  @Override
  public void Execute(TaskConfig config){
    Configuration conf = new Configuration();

    Job job = new Job(conf, "wordcount");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(WordMapper.class);
    job.setReducerClass(CountReduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, config.getInputPath());
    FileOutputFormat.setOutputPath(job, config.getOutputPath());
        
    job.waitForCompletion(true);
  }
}

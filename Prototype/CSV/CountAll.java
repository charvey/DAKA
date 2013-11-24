import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.io.*;

public class CountAll 
{
	public static class Map extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> 
	{

		public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) 
			throws IOException 
		{
			String line = value.toString();
			String [] values = line.split(",");
			for(String s : values)
			{
				String temp = s.trim();
				if(temp.length() > 0)
				{
					output.collect(new Text(temp), new IntWritable(1));
				}
			}
		}
	}
   
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> 
	{
		public void reduce(final Text key, final Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) 
			throws IOException 
		{
			int sum = 0;
			while (values.hasNext()) 
			{
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
     }
   }

	public static void main(String[] args) throws Exception 
	{
		JobConf conf = new JobConf(CountAll.class);
		conf.setJobName("pipcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

	    FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
   }
}

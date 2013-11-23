import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.io.*;

public class CountPeople 
{
	public static class Map extends MapReduceBase implements Mapper<Object, Text, Person, IntWritable> 
	{

		public void map(Object key, Text value, OutputCollector<Person, IntWritable> output, Reporter reporter) 
			throws IOException 
		{
			String line = value.toString();
			String [] values = line.split(",");
			output.collect(new Person(values[25], values[40], values[45]), new IntWritable(1));
		}
	}
   
	public static class Reduce extends MapReduceBase implements Reducer<Person, IntWritable, Person, IntWritable> 
	{
		public void reduce(final Person key, final Iterator<IntWritable> values, OutputCollector<Person, IntWritable> output, Reporter reporter) 
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
		JobConf conf = new JobConf(CountPeople.class);
		conf.setJobName("pipcount");

		conf.setMapOutputKeyClass(Person.class);
		conf.setMapOutputValueClass(IntWritable.class);

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

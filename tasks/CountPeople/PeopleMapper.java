import java.io.IOException;

import daka.io.CSVParser;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

public class PeopleMapper
{
	public static class Map extends Mapper<LongWritable, Text, Person, IntWritable> 
	{
		public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
		{
			String line = value.toString();
			CSVParser parser = new CSVParser();
			String [] values = parser.parseLine(line);
			context.write(new Person(values[25], values[40], values[45]), new IntWritable(1));
		}
	}
}

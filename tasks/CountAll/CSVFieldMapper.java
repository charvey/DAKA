
import daka.io.CSVParser;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CSVFieldMapper {
	public static class Map extends Mapper<Object,Text,Text,IntWritable>{
		public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException
		{
			String line = value.toString();
			CSVParser parser = new CSVParser();
			String [] values = parser.parseLine(line);
			for(String s : values)
			{
				String temp = s.trim();
				if(temp.length() > 0)
				{
					context.write(new Text(temp), new IntWritable(1));
				}
			}
		}
	}
}

package tasks.ClassifyPeople;

import daka.compute.classification.Feature;
import daka.compute.classification.LegacyFeatureMapper;
import daka.compute.classification.Vector;
import daka.compute.classification.LegacyVectorMapper;
import daka.io.CSVParser;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PersonMapper
{
	public static class LabelMapper extends LegacyFeatureMapper
	{
		public void map(LongWritable key, Text value, OutputCollector<LongWritable,Feature> output, Reporter reporter) 
			throws IOException
		{
			Person p=parse(value);
			output.collect(key, new Feature(p.getProduct()));
		}
	}

	public static class FeaturesMapper extends LegacyVectorMapper
	{
		public void map(LongWritable key, Text value, OutputCollector<Feature,Vector> output, Reporter reporter)
			throws IOException
		{
			Person p=parse(value);
			output.collect(new Feature(p.getId()), p.getVector());
		}
	}

	private static Person parse(Text value)
	{
		Person p=null;
		try
		{
			System.out.println(value.toString());
			String line = value.toString();
			CSVParser parser = new CSVParser();
			String [] values = parser.parseLine(line);
			p=new Person(values[0], values[25], values[40], values[45], values[7]);
		} catch(IOException ex){
		}

		return p;
	}
}

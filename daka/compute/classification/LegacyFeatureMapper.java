package daka.compute.classification;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public abstract class LegacyFeatureMapper extends MapReduceBase implements Mapper<LongWritable,Text,LongWritable,Feature>
{
	public abstract void map(LongWritable key, Text value, OutputCollector<LongWritable,Feature> output, Reporter reporter)
		throws IOException;
}

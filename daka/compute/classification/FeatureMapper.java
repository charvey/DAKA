package daka.compute.classification;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public abstract class FeatureMapper extends Mapper<LongWritable,Text,LongWritable,Feature>
{
	public abstract void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException;
}

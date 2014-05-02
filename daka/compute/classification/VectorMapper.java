package daka.compute.classification;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public abstract class VectorMapper extends Mapper<LongWritable,Text,Feature,Vector>
{
	public abstract void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException;
}

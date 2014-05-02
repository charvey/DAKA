package daka.compute.classification.lottery;

import daka.compute.classification.Feature;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

class LotteryTrainer
{
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Feature, Feature, LongWritable>
	{
		public void map(LongWritable key, Feature value, OutputCollector<Feature,LongWritable> output, Reporter reporter)
			throws IOException
		{
			output.collect(value, new LongWritable(1));
		}
	}
}

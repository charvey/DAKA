package daka.compute;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CountReduce
{
	public static class Reduce extends org.apache.hadoop.mapreduce.Reducer<Writable, LongWritable, Writable, LongWritable> 
	{
		public void reduce(Writable key, Iterable<LongWritable> values, Context context) 
			throws IOException, InterruptedException 
		{
			long sum = 0;
			for(LongWritable value : values)
			{
				sum += value.get();
			}
			context.write(key, new LongWritable(sum));
		}
	}

	public static class LegacyReduce extends MapReduceBase implements Reducer<Writable, LongWritable, Writable, LongWritable> 
	{
		public void reduce(Writable key, Iterator<LongWritable> values, OutputCollector<Writable,LongWritable> output, Reporter reporter)
			throws IOException 
		{
			long sum = 0;
			while (values.hasNext())
			{
				sum += values.next().get();
			}
			output.collect(key, new LongWritable(sum));
		}
	}
}

package daka.compute.classification.naiveBayes;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class BayesReducer extends MapReduceBase implements Reducer<Person, LongWritable, Person, LongWritable>
{
	public void reduce(final Person key, final Iterator<LongWritable> values, OutputCollector<Person, LongWritable> output, Reporter reporter) throws IOException
	{
		long sum = 0;
		while(values.hasNext())
			sum += values.next().get();
		
		output.collect(key, new LongWritable(sum));
	}
}

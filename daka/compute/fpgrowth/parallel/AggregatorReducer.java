package daka.compute.fpgrowth.parallel;

import daka.compute.fpgrowth.TopKStringPatterns;
import daka.compute.helpers.Parameters;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * groups all Frequent Patterns containing an item and outputs the top K
 * patterns containing that particular item
 * 
 */
public class AggregatorReducer extends
		Reducer<Text, TopKStringPatterns, Text, TopKStringPatterns>
{

	private int maxHeapSize = 50;

	@Override
	protected void reduce(Text key, Iterable<TopKStringPatterns> values,
			Context context) throws IOException, InterruptedException
	{
		TopKStringPatterns patterns = new TopKStringPatterns();
		for (TopKStringPatterns value : values)
		{
			context.setStatus("Aggregator Reducer: Selecting TopK patterns for: " + key);
			patterns = patterns.merge(value, maxHeapSize);
		}
		context.write(key, patterns);

	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException
	{
		super.setup(context);
		Parameters params = new Parameters(context.getConfiguration().get("pfp.parameters", ""));
		maxHeapSize = Integer.valueOf(params.get("maxHeapSize", "50"));

	}
}

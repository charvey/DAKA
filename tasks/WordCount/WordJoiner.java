import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;

public class WordJoiner
{
	public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> 
	{
		public void reduce(LongWritable key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException
		{
			String line = "";
			for(Text word : values)
			{
				line += (" "+ word.toString());
			}
			context.write(null, new Text(line));
		}
	}
}

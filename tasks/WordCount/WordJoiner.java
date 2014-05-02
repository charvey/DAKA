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
			StringBuilder builder=new StringBuilder();
			for(Text word : values)
			{
				builder.append(" "+ word.toString());
			}
			builder.deleteCharAt(0);
			context.write(null, new Text(builder.toString()));
		}
	}
}

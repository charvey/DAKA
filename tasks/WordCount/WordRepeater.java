
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordRepeater {
	public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException
		{
			for(long i=0; i<value.getLength(); i++) {
				context.write(new LongWritable(key.get()+i), value);
			}
		}
	}
}

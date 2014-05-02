package daka.compute.classification.lottery;

import daka.compute.classification.Feature;
import daka.compute.classification.Vector;

import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

class LotteryClassifier
{
	public static class Map extends MapReduceBase implements Mapper<Writable, Vector, Writable, Feature>
	{
		private static LotteryModel model;

		public void configure(JobConf conf)
		{
			try
			{
				Path localFile=DistributedCache.getLocalCacheFiles(conf)[0];
				model=new LotteryModel(localFile);
			} catch(IOException ex) {
				System.err.println(ex.getMessage());
			}
			
		}

		public void map(Writable key, Vector value, OutputCollector<Writable,Feature> output, Reporter reporter)
			throws IOException
		{
			Feature label=model.drawTicket();
			output.collect(key,label);
		}
	}
}

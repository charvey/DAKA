package daka.compute.classification.lottery;

import daka.compute.classification.Feature;
import daka.compute.classification.LegacyFeatureMapper;
import daka.compute.classification.Vector;
import daka.compute.classification.LegacyVectorMapper;
import daka.compute.CountReduce;
import daka.core.TaskConfig;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class LotteryClassification
{
	public void Train(TaskConfig config, Class<? extends LegacyVectorMapper> vmap, Class<? extends LegacyFeatureMapper> fmap)
	{
		try
		{
			JobConf jobConf = new JobConf(config.getConfig(), LotteryClassification.class);
			jobConf.setJobName("lotterytrainer");

			jobConf.setInputFormat(TextInputFormat.class);
			jobConf.setOutputFormat(TextOutputFormat.class);

			JobConf map1Conf=new JobConf(false); 
			ChainMapper.addMapper(jobConf,fmap,
				LongWritable.class,Text.class,
				LongWritable.class,Feature.class,
				false,map1Conf);
			JobConf map2Conf=new JobConf(false);
			ChainMapper.addMapper(jobConf,LotteryTrainer.Map.class,
				LongWritable.class,Feature.class,
				Feature.class,LongWritable.class,
				false,map2Conf);
			JobConf redConf=new JobConf(false);
			ChainReducer.setReducer(jobConf,CountReduce.LegacyReduce.class,
				Feature.class,LongWritable.class,
				Feature.class,LongWritable.class,
				false,redConf);

			FileInputFormat.addInputPath(jobConf, new Path(config.getInputPath()));
			FileOutputFormat.setOutputPath(jobConf, new Path(config.getOutputPath()));

			JobClient.runJob(jobConf);
		} catch(IOException ex){
			System.err.println(ex.getMessage());
		}
	}

	public void Classify(TaskConfig config, Class<? extends LegacyVectorMapper> vmap)
	{
		try
		{
			String modelStr=config.getArguments().get("m");
			URI modelURI=new URI(modelStr);
			DistributedCache.addCacheFile(modelURI, config.getConfig());

			JobConf jobConf = new JobConf(config.getConfig(), LotteryClassification.class);
			jobConf.setJobName("lotteryclassifier");

			jobConf.setInputFormat(TextInputFormat.class);
			jobConf.setOutputFormat(TextOutputFormat.class);

			JobConf map1Conf=new JobConf(false); 
			ChainMapper.addMapper(jobConf,vmap,
				LongWritable.class,Text.class,
				Feature.class,Vector.class,
				false,map1Conf);
			JobConf map2Conf=new JobConf(false);
			ChainMapper.addMapper(jobConf,LotteryClassifier.Map.class,
				Feature.class,Vector.class,
				Feature.class,Feature.class,
				false,map2Conf);

			FileInputFormat.addInputPath(jobConf, new Path(config.getInputPath()));
			FileOutputFormat.setOutputPath(jobConf, new Path(config.getOutputPath()));

			JobClient.runJob(jobConf);		
		} catch(IOException ex){
			System.err.println(ex.getMessage());
		} catch(Exception ex){
		}
	}
}

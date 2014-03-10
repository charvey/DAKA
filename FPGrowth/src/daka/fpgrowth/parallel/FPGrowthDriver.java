package daka.fpgrowth.parallel;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Sets;
import com.google.common.io.Closeables;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import daka.helpers.*;

import daka.fpgrowth.TopKStringPatterns;
import daka.fpgrowth.FPGrowth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FPGrowthDriver extends Configured implements Tool
{

	private static final Logger log = LoggerFactory
			.getLogger(FPGrowthDriver.class);

	private FPGrowthDriver()
	{
	}

	public static void main(String[] args) throws Exception
	{
		ToolRunner.run(new Configuration(), new FPGrowthDriver(), args);
	}

	/**
	 * Run TopK FPGrowth given the input file,
	 */
	@Override
	public int run(String[] args) throws Exception
	{

		Parameters params = new Parameters();

		params.set("minSupport", "100");
		params.set("maxHeapSize", "50");
		params.set("numGroups", "1000");
		params.set("treeCacheSize", "4000");
		String encoding = "UTF-8";
		params.set("encoding", encoding);

		

		Path inputDir = new Path("hdfs://localhost:9000/user/sgrey/input");
		Path outputDir = new Path("hdfs://localhost:9000/user/sgrey/output");

		params.set("input", inputDir.toString());
		params.set("output", outputDir.toString());

	
		Configuration conf = new Configuration();
		FileUtil.delete(conf, outputDir);
		PFPGrowth.runPFPGrowth(params);
		

		return 0;
	}

}

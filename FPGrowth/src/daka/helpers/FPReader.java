package daka.helpers;

import java.io.File;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;
import java.util.ArrayList;

import com.google.common.base.Charsets;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Utils.OutputFileUtils.OutputFilesFilter;
import org.apache.hadoop.util.Tool;

public final class FPReader extends Configured implements Tool
{

	public FPReader()
	{
		setConf(new Configuration());
	}

	@Override
	public int run(String[] args) throws Exception
	{

		Path[] pathArr;
		Configuration conf = new Configuration();
		Path input = new Path(
				"hdfs://localhost:9000/user/sgrey/output/frequentpatterns/part-r-00000");
		FileSystem fs = input.getFileSystem(conf);
		if (fs.getFileStatus(input).isDir())
		{
			pathArr = FileUtil.stat2Paths(fs.listStatus(input, new OutputFilesFilter()));
		} else
		{
			pathArr = new Path[1];
			pathArr[0] = input;
		}

		Writer writer;
		boolean shouldClose;

		
		writer = new OutputStreamWriter(System.out, Charsets.UTF_8);
		
		

		for (Path path : pathArr)
		{

			int sub = Integer.MAX_VALUE;

			SequenceFileIterator<?, ?> iterator = new SequenceFileIterator<Writable, Writable>(path, true, conf);

			long count = 0;

			
				long numItems = Long.MAX_VALUE;

					numItems = Integer.MAX_VALUE;
				
				
				while (iterator.hasNext() && count < numItems)
				{
					Pair<?, ?> record = iterator.next();
					String key = record.getFirst().toString();
					writer.append("Key: ").append(key);
					String str = record.getSecond().toString();
					writer.append(": Value: ").append(
							str.length() > sub ? str.substring(0, sub)
									: str);
					writer.write('\n');
					count++;
				}
				
		
		}
		writer.flush();

		

		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		new FPReader().run(args);
	}

}

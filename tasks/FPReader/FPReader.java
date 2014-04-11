
import com.google.common.base.Charsets;

import daka.compute.CountReduce;
import daka.core.Task;
import daka.core.TaskConfig;

import java.io.File;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Utils.OutputFileUtils.OutputFilesFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FPReader extends Task {
	@Override
	public void PreExecute(TaskConfig config){}

	@Override
	public void Execute(TaskConfig config){
		try{
			Path[] pathArr;
			Configuration conf = new Configuration();
			conf.set("fs.default.name", "hdfs://localhost:9000");
			conf.set("mapred.job.tracker", "localhost:9001");
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
				
				
					while (iterator.hasNext() && count < numItems)
					{
						Pair<?, ?> record = iterator.next();
						String key = record.getFirst().toString();
						writer.append("Key: ").append(key);
						String str = record.getSecond().toString();
						writer.append(": Value: ").append(
							str.length() > sub
								? str.substring(0, sub)
								: str
						);
						writer.write('\n');
						count++;
					}
			}
			writer.flush();
		} catch(IOException ex){
		} catch(InterruptedException ex){
		} catch(ClassNotFoundException ex){
		} catch(Exception ex){
		}
	}

	@Override
	public void PostExecute(TaskConfig config){}
}

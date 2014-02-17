package daka.core;

import java.util.Dictionary;

import org.apache.hadoop.fs.Path;

public class TaskConfig {
	private String inputPath;
	private String outputPath;
	private String arguments;

	public String getInputPath(){
		return inputPath;
	}
	public String getOutputPath(){
		return outputPath;
	}

	public TaskConfig(String inputPath, String outputPath,
		Dictionary<String,String> arguments)
	{
		this.inputPath=inputPath;
		this.outputPath=outputPath;
	}
}

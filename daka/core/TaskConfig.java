package daka.core;

import java.util.Dictionary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class TaskConfig {
	private String inputPath;
	private String outputPath;
	private Dictionary<String,String> arguments;
	private Configuration config;

	public String getInputPath(){
		return inputPath;
	}
	public String getOutputPath(){
		return outputPath;
	}
	public Dictionary<String,String> getArguments(){
		return arguments;
	}
	public Configuration getConfig(){
		return config;
	}

	public TaskConfig(String inputPath, String outputPath,
		Dictionary<String,String> arguments, Configuration config)
	{
		this.inputPath=inputPath;
		this.outputPath=outputPath;
		this.arguments=arguments;
		this.config=config;
	}
}

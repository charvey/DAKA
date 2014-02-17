package daka.core;

import org.apache.hadoop.fs.Path;

public class TaskConfig {
	private String inputPath;
	private String outputPath;


	public String getInputPath(){
		return inputPath;
	}
	public String getOutputPath(){
		return outputPath;
	}

	public TaskConfig(String inputPath, String outputPath){
		this.inputPath=inputPath;
		this.outputPath=outputPath;
	}
}

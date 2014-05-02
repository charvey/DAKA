package tasks.ClassifyPeople;

import daka.compute.classification.lottery.LotteryClassification;
import daka.compute.CountReduce;
import daka.core.Task;
import daka.core.TaskConfig;
import daka.io.FileInput;
import daka.io.FileOutput;
import daka.util.FileUtil;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class ClassifyPeople extends Task {
	@Override
	public void PreExecute(TaskConfig config){
		FileInput inputFile=null,modelFile=null;

		String inputFilePath=config.getArguments().get("if");
		if(inputFilePath!=null)
		{
			System.out.println("Input file: "+inputFilePath);
			inputFile=new FileInput(inputFilePath,config.getInputPath(),config.getConfig());
		}
		String modelFilePath=config.getArguments().get("mf");
		if(modelFilePath!=null)
		{
			System.out.println("Model file: "+modelFilePath);
			String modelPath=config.getArguments().get("m");
			modelFile=new FileInput(modelFilePath,modelPath,config.getConfig());			
		}

		try{
			if(inputFile!=null)
			{
				inputFile.Update();
			}
			if(modelFile!=null)
			{
				modelFile.Update();
			}

			FileUtil.delete(config.getConfig(), new Path(config.getOutputPath()));
		} catch(IOException ex){
			System.err.println(ex.getMessage());
		}
	}

	@Override
	public void Execute(TaskConfig config){
		if("true".equals(config.getArguments().get("train")))
		{
			LotteryClassification lc=new LotteryClassification();

			lc.Train(config, PersonMapper.FeaturesMapper.class, PersonMapper.LabelMapper.class);
		}
		else if("true".equals(config.getArguments().get("classify")))
		{
			LotteryClassification lc=new LotteryClassification();

			lc.Classify(config, PersonMapper.FeaturesMapper.class);
		}
	}

	@Override
	public void PostExecute(TaskConfig config){
		String outputFile=config.getArguments().get("of");
		if(outputFile!=null)
		{
			String outputFilePath=config.getOutputPath()+"/part-00000";
			System.out.println("Output file: "+outputFilePath);
			FileOutput fo=new FileOutput(outputFilePath,outputFile,config.getConfig());

			try{
				fo.Update();
			} catch(IOException ex){
				System.err.println(ex.getMessage());
			}
		}
	}
}

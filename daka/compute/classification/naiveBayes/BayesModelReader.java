package daka.compute.classification.naiveBayes;

import daka.io.CSVParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;

public class BayesModelReader 
{		
	public BayesModel LoadModel(String modelpath) throws Exception
	{
		BayesModel model = new BayesModel();
		PrintWriter writer = new PrintWriter("loadModelLog.txt", "UTF-8");
		
		BufferedReader reader = new BufferedReader(new FileReader("bayesModel.txt"));
		String line = null;
		CSVParser parser = new CSVParser();
		writer.println("Going to the while and read the bayesModel.txt");
		while ((line = reader.readLine()) != null) 
		{
			String []tuple = line.split("\t");
			writer.println("from "+line+" got tuples: "+tuple[0] + " |and| "+tuple[1]);
			String []values = parser.parseLine(tuple[0]);
			
			// Increment the views for the category
			if(values[0] != null && !IsOnlyWhiteSpace(values[0]))
			{
				model.IncrementLabel(values[0], Long.parseLong(tuple[1], 10));
			}
			
			//increment the views for the of this features for this category
			for(int i = 1; i< 4; i++)
				if(!IsOnlyWhiteSpace(values[i]))
				{
					model.IncrementFeature(values[0], values[i], Long.parseLong(tuple[1], 10));
				}
			writer.println();
		}
		
		writer.close();
		reader.close();
		return model;
	}
	
	private boolean IsOnlyWhiteSpace(String value)
	{
		return value.trim().length() == 0;
	}

}

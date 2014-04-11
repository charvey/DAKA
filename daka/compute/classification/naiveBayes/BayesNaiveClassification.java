
import java.io.*;

import org.apache.hadoop.fs.Path;


public class BayesNaiveClassification 
{
	public static void main(String[] args) throws Exception
	{
		System.out.println("Entra al main");
		BayesModelReader modelReader = new BayesModelReader();
		BayesClassifier classifier = new BayesClassifier();
		System.out.println("va a leer el model");
		BayesModel model = modelReader.LoadModel("bayesModel.txt");
		PrintWriter writer = new PrintWriter("bayesResult.txt", "UTF-8");
		
		writer.println("total categories: "+ model.getLabelCounts().keySet().size());
		
		BufferedReader reader = new BufferedReader(new FileReader("10000.csv"));
		String line = null;
		CSVParser parser = new CSVParser();
	
		PrintWriter writer1 = new PrintWriter("Classifierlog.txt", "UTF-8");
		while((line = reader.readLine()) != null)
		{
			line = reader.readLine();
			String []values = parser.parseLine(line);
			
			String [] toclassify = new String[3];
			toclassify[0] = values[25]; //gender
			toclassify[1] = values[40]; //state
			toclassify[2] = values[45]; //smoker
			
			BayesClassifierResult result = classifier.Classify(model, values, "Unknown", writer1);
			
			writer.println(result.toString());
		}
		writer.close();
		writer1.close();
		reader.close();
	}	
}

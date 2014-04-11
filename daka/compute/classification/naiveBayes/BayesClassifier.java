
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Collection;



public class BayesClassifier 
{		
	//Classify a tuple according to the BayesModel
	public BayesClassifierResult Classify(BayesModel model, String[] tuple, String defaultCategory, PrintWriter writer) throws Exception
	{
		BayesClassifierResult result = new BayesClassifierResult(defaultCategory);
		double max = 0;
		Collection<String> categories = model.getLabels();
		
		// FOr each of the labels ge the prob and check if is greater than ma
		for(String category : categories)
		{
			double prob = Probability(model, category, tuple, writer);
			if(prob>max)
			{
				max = prob;
				result.setCategory(category);
				result.setSubject(tuple);
				result.setScore(max);
			}
		}
		return result;
	}
	
	protected double TupleProbability(BayesModel model, String label, String[] tuple) throws Exception
	{
		return TupleProbability(model, label, tuple);
	}
	
	protected double TupleProbability(BayesModel model, String label, String[] tuple, PrintWriter writer) throws IOException, UnsupportedEncodingException
	{
		double result = 1;
		double [] numSeen = new double [tuple.length];
		double labelCount = model.getLabelCounts().get(label);
		
		Map<String, Map<String, Long>> feat_counts = model.getFeatureCounts();
		
		for(int i=0; i< tuple.length; i++)
		{
			Map<String, Long> labelFeatSeen = feat_counts.get(tuple[i]);
			if(labelFeatSeen == null)
			{
				writer.println("Feature was null "+tuple[i]+" !!!!!!!!!!!!!!!!!");
				// apply Laplacian correction
				for(int j=0; j< tuple.length; j++)	
					numSeen[j] += 1;	
			   labelCount += tuple.length;
			}
			else
			{
				Long count = labelFeatSeen.get(label);
				if(count == null)
				{
					// apply Laplacian correction
					for(int j=0; j< tuple.length; j++)	
						numSeen[j] += 1;
					labelCount += tuple.length;
				}
				else
				{
					numSeen[i] = count;
				}
			}
		}
		
		// Calculate the prob of the tuple with the laplacian correction applied
		writer.println("Calculate the prob of the tuple with the laplacian correction applied");
		for(int i=0; i< tuple.length; i++)
		{
			result *= numSeen[i]/labelCount;
			writer.print(numSeen[i]/labelCount + " * ");
		}
		
		writer.println();
		
		
		double probOFLabel = (double)model.getLabelCounts().get(label) / (double)model.getTotalLabelCounts();
		writer.println("Total data count:"+probOFLabel);
		
		result *= probOFLabel;
		writer.println("result (*= probOFLabel):"+result);
		//double result = 1;
		//for(String feature : tuple)
		//{
		//	result *= model.WeightedFeatureProbability(label, feature, weight, defaultProb);
		//}
		//
		//result *= model.WeightedLabelProbability(label);
		//
		//return result;
		return result;
	}
	
	
	//Calculate the probability that the label is the label of the document
	public double Probability(BayesModel model, String label, String[] tuple, PrintWriter writer ) throws Exception
	{
		//return TupleProbability(model, label, tuple, weight, defaultProb) * (((double) model.getCategoryCount(label)) / ((double)model.getTotalLabelCounts()));
		return TupleProbability(model, label, tuple, writer);
	}
}

























































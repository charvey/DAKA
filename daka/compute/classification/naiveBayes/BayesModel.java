package daka.compute.classification.naiveBayes;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class BayesModel 
{
	public static final double DEFAULT_PROBABILITY = 1f;
	public static final double DEFAULT_WEIGHT = 1.0f;
	
	// key is the label, value is the member of times we have seen that label
	Map<String, Long> labelCounts = new HashMap<String, Long>();
	
	// key is the feature, value is the map. Inner map key is the label, value is the number of times the feature has been seen with that label
	Map<String, Map<String, Long>> featureCounts = new HashMap<String, Map<String, Long>>();
	
	/**
	 * Increment the amount of appearances of a label 
	 * 
	 *  @param label The label to be incremented
	 *  */
	public void IncrementLabel(String label)
	{
		Long count = labelCounts.get(label);
		
		if(count == null)
			count = new Long(1);
		else
			count++;
		
		labelCounts.put(label, count);
	}
	
	public void IncrementLabel(String label, long increment)
	{
		Long count = labelCounts.get(label);
		
		if(count == null)
			count = new Long(increment);
		else
			count+=increment;
		
		labelCounts.put(label, count);
	}
	
	public void setLabelCount(String label, long count)
	{
		labelCounts.put(label, new Long(count));
	}
	
	/**
	 * @param label The label to look up
	 * @return The number of times the label has been seen, 0 if hasn't been seen
	 * */
	public long getCategoryCount(String label)
	{
		Long count = labelCounts.get(label);
		
		return count != null ? count : 0;
	}
	
	public long getLabelFeatureCount(String feature, String label)
	{
		long result = 0;
		Map<String, Long> map = featureCounts.get(feature);
		
		if(map != null)
		{
			Long count = map.get(label);
			if(count != null)
				result = count;
		}
		
		return result;
	}
	
	/**
	 * @return The total number of labels
	 * */
	public long getTotalLabelCounts()
	{
		long result = 0;
		for(Long vals : labelCounts.values())
				result += vals;
		
		return result;
	}
	
	/**
	 * @param label the label the feature belongs to
	 * @param feature the feature of the label
	 * @param count the number of times the feature for this label has been seen
	 * 
	 * */
	public void setFeatureCount(String label, String feature, long count)
	{
		Map<String, Long> map = featureCounts.get(feature);
		
		if(map == null)
		{
			map = new HashMap<String, Long>();
			featureCounts.put(feature, map);
		}
		
		map.put(label, new Long(count));
	}
	
	/**
	 * Increment the feature for this label. Should only be called for a given feature once per tuple
	 * 
	 * @param label The label for this feature
	 * @param feature The feature
	 * */
	public void IncrementFeature(String label, String feature)
	{
		Map<String, Long> map = featureCounts.get(feature);
		Long count;
		
		if(map == null)
		{
			map = new HashMap<String, Long>();
			featureCounts.put(feature, map);
			count = new Long(1);
		}
		else
		{
			count = map.get(label);
			if(count == null)
				count = new Long(1);
			else
				count++;
		}
		
		map.put(label, count);
	}
	
	public void IncrementFeature(String label, String feature, long increment)
	{
		feature = feature.trim();
		label = label.trim();
		Map<String, Long> map = featureCounts.get(feature);
		Long count;
		
		if(map == null)
		{
			map = new HashMap<String, Long>();
			featureCounts.put(feature, map);
			count = new Long(increment);
		}
		else
		{
			count = map.get(label);
			if(count == null)
				count = new Long(increment);
			else
				count+=increment;
		}
		
		map.put(label, count);
	}
	
	/**
	 * Get the weighted feature probability, using the default weight and default assumed probability
	 * 
	 * @param label The label of the feature
	 * @param feature The feature to calculate the weighted probability for
	 * @return the probability
	 * */
	public double WeightedFeatureProbability(String label, String feature)
	{
		return WeightedFeatureProbability(label, feature, DEFAULT_WEIGHT, DEFAULT_PROBABILITY);
	}
	
	/**
	 * Get the weighted feature probability
	 * 
	 * @param label The label of the feature
	 * @param feature The feature to calculate the weighted probability for
	 * @return the probability
	 * */
	public double WeightedFeatureProbability(String label, String feature, double weight, double defaultProb)
	{
		double prob = FeatureProbability( label, feature);
	
		//double unweighted = FeatureProbability(label, feature);
		//long totalNumSeen = getTotalNumSeen(feature);
		
		//return ((weight*defaultProb) + (totalNumSeen*unweighted)) / (weight+totalNumSeen);
		return prob;
	}
	
	private long getTotalNumSeen(String feature)
	{
		long result = 0;
		Map<String, Long> featureMap = featureCounts.get(feature);
		
		if(featureMap != null)
		{
			for(Long count : featureMap.values())
				result += count;
		}
		
		return result;
	}
	
	public double FeatureProbability(String label, String feature)
	{
		Long labelCount = labelCounts.get(label);
		Map<String, Long> featMap = featureCounts.get(feature);
		double result = 0;
		if(featMap != null && labelCount != null)
		{
			Long numSeen = featMap.get(label);
			if(numSeen != null)
				result = ((double)numSeen) / ((double)labelCount);
		}
		
		return result;
	}
	
	public double WeightedLabelProbability(String label)
	{
		Long labelCount = labelCounts.get(label);
		
		long count = 0;
		for(String feature : featureCounts.keySet())
		{
			 count += getTotalNumSeen(feature);
		}
		
		count -= labelCount;
		
		return DEFAULT_WEIGHT * labelCount / (DEFAULT_WEIGHT + count );
	}
	
	public Map<String, Map<String,Long>> getFeatureCounts()
	{
		return featureCounts;
	}
	
	public Map<String, Long> getLabelCounts()
	{
		return labelCounts;
	}
	
	public Collection<String> getLabels()
	{
		return labelCounts.keySet();
	}
}



















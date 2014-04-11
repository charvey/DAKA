

public class BayesClassifierResult 
{
	private String category;
	private double score;
	String[] features;
	
	public BayesClassifierResult()
	{
		
	}
	
	public BayesClassifierResult(String category)
	{
		this.category = category;
	}
	
	public BayesClassifierResult(String category, double score)
	{
		this.category = category;
		this.score = score;
	}
	
	public void setSubject(String[] features)
	{
		this.features = features;
	}
	
	public String getCategory()
	{
		return category;
	}
	
	public double getScore()
	{
		return score;
	}
	
	public void setCategory(String category)
	{
		this.category = category;
	}
	
	public void setScore(double score)
	{
		this.score = score;
	}
	
	public String toString()
	{
		String resp ="";
		resp += "For: " ;
		if(features != null)
		for(String feat : features)
		{
			resp += feat +", ";
		}
		resp+= "\n";
		 resp+= "ClassifierResult: "+
				"category='"+ category + '\''+
				", score="+ score ;
		
		return resp;
	}
}

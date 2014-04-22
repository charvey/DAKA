package daka.compute.classification.naiveBayes;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.WritableComparable;

public class Person implements WritableComparable
{
	String features[];
	
	public Person(){}
	
	public Person(String[] features)
	{
		this.features = features;
	}

	public void readFields(DataInput in) throws IOException
	{
		int length = 25; // WE DONT KNOW A PRIORI THE LENGTH !!!!! 
		
		features = new String[length];
		
		for(int i = 0; i< length ; i++)
		{
			features[i] = in.readUTF();
		}
	}
	public void write(DataOutput out) throws IOException
	{
		for(String feature : features)
		{
			out.writeUTF(feature);
		}
	}
	@Override
    public boolean equals(Object o) 
	{
		if(o instanceof Person)
		{
			Person p = (Person)o;
			 
			for(int i =0; i< p.getFeatures().length; i++)
				if(i < features.length)
				{
					if (!features[i].equals(p.features[i]))
						return false;
				}
				else
					return false;
		}
		else
			return false;
		
		return true;
	}

	public int compareTo(Object o)
	{
		if(!(o instanceof Person))
		{
			return -1;
		}
		
		Person p = (Person)o;
		for(int i =0; i< p.getFeatures().length; i++)
			if(i < features.length)
			{
				int comp = features[i].compareTo(p.features[i]);
				if (comp != 0)
					return comp;
			}
			else
				return -1;
		
		return 0;
	}
	public String toString()
	{
		String result ="";
		for(String feats : features)
		{
			result += feats + ", ";
		}
		return result;
	}
	
	public String[] getFeatures()
	{
		return features;
	}
}

package tasks.ClassifyPeople;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import daka.compute.classification.Feature;
import daka.compute.classification.Vector;

public class Person
{
	private String id, gender, state, smoker, product;

	public Person(){}

	public Person(String id, String gender, String state, String smoker, String product)
	{
		this.id=id;
		this.gender = gender;
		this.state = state;
		this.smoker = smoker;
		this.product=product;
	}

	public String getId()
	{
		return this.id;
	}

	public Vector getVector()
	{
		Feature[] features=new Feature[3];
		features[0]=new Feature(gender);
		features[1]=new Feature(state);
		features[2]=new Feature(smoker);
		return new Vector(features);
	}

	public String getProduct()
	{
		return product;
	}
}

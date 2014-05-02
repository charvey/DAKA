package daka.compute.classification;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Vector implements Writable
{
	private Feature[] _features;

	public Vector(Feature[] features)
	{
		this._features=features;
	}

	public void readFields(DataInput in) throws IOException
	{
		_features=new Feature[in.readInt()];
		for(int i=0; i<_features.length; i++)
		{
			_features[i]=new Feature(in);
		}
	}
	public void write(DataOutput out) throws IOException
	{
		out.writeInt(_features.length);
		for(int i=0; i<_features.length; i++)
		{
			_features[i].write(out);
		}
	}

}

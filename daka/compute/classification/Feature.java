package daka.compute.classification;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Feature implements WritableComparable
{
	private char _type;
	private double _numerical;
	private String _categorical;

	public Feature(){}
	public Feature(DataInput in) throws IOException
	{
		readFields(in);
	}
	public Feature(double value)
	{
		this._type='N';
		this._numerical=value;
	}
	public Feature(String value)
	{
		this._type='C';
		this._categorical=value;
	}

	public boolean isNumerical()
	{
		return _type=='N';
	}
	public boolean isCategorical()
	{
		return _type=='C';
	}
	
	public double getNumerical()
	{
		if(isNumerical())
		{
			return _numerical;
		}
		throw new IllegalStateException();
	}	
	public String getCategorical()
	{
		if(isCategorical())
		{
			return _categorical;
		}
		throw new IllegalStateException();
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		this._type=in.readChar();
		if(this._type=='C')
		{
			this._categorical=in.readUTF();
		}
		else if(this._type=='N')
		{
			this._numerical=in.readDouble();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeChar(this._type);
		if(this._type=='C')
		{
			out.writeUTF(this._categorical);
		}
		else if(this._type=='N')
		{
			out.writeDouble(this._numerical);
		}
	}

	@Override
	public int compareTo(Object o) {
		if(!(o instanceof Feature))
		{
			return -1;
		}
		Feature f = (Feature)o;
		if(isNumerical()&&f.isNumerical())
		{
			return ((Double)getNumerical()).compareTo(f.getNumerical());
		}
		if(isCategorical()&&f.isCategorical())
		{
			return getCategorical().compareTo(f.getCategorical());
		}
		return 0;
       }


	@Override
	public int hashCode() {
		if(isNumerical())
		{
			return ((Double)getNumerical()).hashCode();
		}
		else
		{
			return getCategorical().hashCode();
		}
	}

	@Override
	public String toString()
	{
		if(isNumerical())
		{
			return "N:"+getNumerical();
		}
		else
		{
			return "C:"+getCategorical();
		}
	}

	public static Feature fromString(String s)
	{
		Feature f=null;
		if(s.charAt(0)=='N')
		{
			f=new Feature(Double.parseDouble(s.substring(2)));
		}
		else if(s.charAt(0)=='C')
		{
			f=new Feature(s.substring(2));
		}
		return f;
	}
}

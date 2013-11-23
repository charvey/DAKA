import org.apache.hadoop.io.WritableComparable;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

public class Person implements WritableComparable
{
	String gender, state, smoker;

	public Person(){}

	public Person(String gender, String state, String smoker)
	{
		this.gender = gender;
		this.state = state;
		this.smoker = smoker;	
	}

	public void readFields(DataInput in) throws IOException
	{
		this.gender = in.readUTF();
		this.state = in.readUTF();
		this.smoker = in.readUTF();
	}
	public void write(DataOutput out) throws IOException
	{
		out.writeUTF(this.gender);
		out.writeUTF(this.state);
		out.writeUTF(this.smoker);
	}
	@Override
    public boolean equals(Object o) 
	{
		if(o instanceof Person)
		{
			Person p = (Person)o;
			return gender == p.gender && state == p.state && smoker == p.smoker;
		}
		return false;
	}

	@Override
	public int compareTo(Object o)
	{
		if(!(o instanceof Person))
		{
			return -1;
		}
		Person p = (Person)o;
		int g, st, sm;
		g = gender.compareTo(p.gender);
		if(g != 0)
			return g;
		st = state.compareTo(p.state);
		if(st != 0)
			return st;
		sm = smoker.compareTo(p.smoker);
		if(sm != 0)
			return sm;
		return 0;
	}
	public String toString()
	{
		return gender + ", " + state + ", " + smoker;
	}
}

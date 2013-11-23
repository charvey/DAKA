import org.bson.*;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.hadoop.util.*;
import com.mongodb.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.*;
import java.util.*;

public class StatMapper extends Mapper<Object,BSONObject, Person, IntWritable>
{
	@Override
	public void map(Object key, BSONObject val, final Context context)
		throws IOException, InterruptedException
	{
		String gender = val.get("STASEX").toString();
		String state = val.get("APPSTATE").toString();
		String smoker = val.get("CLASSNAME").toString();

		context.write(new Person(gender, state, smoker), new IntWritable(1));
                
	}

}

import org.bson.*;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.hadoop.util.*;
import com.mongodb.hadoop.io.*;

// Commons
import org.apache.commons.logging.*;

// Hadoop
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

// Java
import java.io.*;
import java.util.*;

public class StatReducer extends Reducer <Person, IntWritable, BSONWritable, IntWritable> 
{
	private static final Log LOG = LogFactory.getLog( StatReducer.class );

    @Override
    public void reduce( final Person pKey, final Iterable<IntWritable> pValues, final Context pContext )
            throws IOException, InterruptedException
	{
        int sum = 0;
        for ( final IntWritable value : pValues )
		{
            sum += value.get();
        }
        BSONObject outDoc = new BasicDBObjectBuilder().start().add( "gender" , pKey.gender).add( "state" , pKey.state ).add("smoker", pKey.smoker).get();
        BSONWritable pkeyOut = new BSONWritable(outDoc);

        pContext.write( pkeyOut, new IntWritable(sum) );
    }


}

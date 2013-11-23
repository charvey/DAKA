import org.bson.*;
import com.mongodb.hadoop.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

public class Stat extends MongoTool 
{
    public static void main( final String[] pArgs ) throws Exception
	{
        System.exit( ToolRunner.run( new Stat(), pArgs ) );
    }
}

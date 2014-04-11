package daka.compute.fpgrowth.parallel;

import daka.compute.helpers.*;
import daka.io.CSVParser;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * maps each transaction to all unique items groups in the transaction. mapper
 * outputs the group id as key and the transaction as value
 * 
 */
public class ParallelFPGrowthMapper extends
		Mapper<LongWritable, Text, IntWritable, TransactionTree>
{

	private final HashMap<String, Integer> fMap = Maps.newHashMap();
	private int maxPerGroup;
	private final IntWritable wGroupID = new IntWritable();
	CSVParser parser = new CSVParser();
	@Override
	protected void map(LongWritable offset, Text input, Context context)
			throws IOException, InterruptedException
	{

		String[] items = parser.parseLine(input.toString());

		Set<Integer> itemSet = Sets.newHashSet();

		for (String item : items)
		{
			if (fMap.containsKey(item) && !item.trim().isEmpty())
			{
				itemSet.add(fMap.get(item));
			}
		}

		ArrayList<Integer> itemArr = new ArrayList<Integer>(itemSet.size());
		itemArr.addAll(itemSet);
		Collections.sort(itemArr);

		Set<Integer> groups = Sets.newHashSet();
		for (int j = itemArr.size() - 1; j >= 0; --j)
		{
			// generate group dependent shards
			int item = itemArr.get(j);
			int groupID = PFPGrowth.getGroup(item, maxPerGroup);

			if (!groups.contains(groupID))
			{
				ArrayList<Integer> tempItems = new ArrayList<Integer>(j + 1);

	
				for(int x = 0; x <= j; ++x)
				{
					try
					{
						tempItems.add(itemArr.get(x));
						//tempItems.set(x, itemArr.get(x));
					}
					catch(Exception e)
					{
						/*System.out.println("x: " + x);
						System.out.println("tempItems size: " + tempItems.size());
						System.exit(1);*/
					}
					
				}
				tempItems.trimToSize();
				context.setStatus("Parallel FPGrowth: Generating Group Dependent transactions for: "
						+ item);
				wGroupID.set(groupID);
				context.write(wGroupID, new TransactionTree(tempItems, 1L));
			}
			groups.add(groupID);
		}

	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException
	{
		super.setup(context);

		int i = 0;
		for (Pair<String, Long> e : PFPGrowth.readFList(context
				.getConfiguration()))
		{
			fMap.put(e.getFirst(), i++);
		}

		Parameters params = new Parameters(context.getConfiguration().get(PFPGrowth.PFP_PARAMETERS, ""));

		maxPerGroup = params.getInt(PFPGrowth.MAX_PER_GROUP, 0);
	}
}

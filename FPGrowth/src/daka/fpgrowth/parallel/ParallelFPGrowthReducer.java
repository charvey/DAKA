package daka.fpgrowth.parallel;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.ArrayList;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import daka.helpers.Pair;
import daka.helpers.Parameters;
import daka.helpers.ContextStatusUpdater;
import daka.helpers.ContextWriteOutputCollector;
import daka.helpers.IntegerStringOutputConverter;
import daka.fpgrowth.FPGrowth;
import daka.fpgrowth.TopKStringPatterns;

/**
 * takes each group of transactions and runs Vanilla FPGrowth on it and outputs
 * the the Top K frequent Patterns for each group.
 * 
 */
public final class ParallelFPGrowthReducer extends
		Reducer<IntWritable, TransactionTree, Text, TopKStringPatterns>
{

	private final List<String> featureReverseMap = Lists.newArrayList();
	private final ArrayList<Long> freqList = new ArrayList<Long>();
	private int maxHeapSize = 50;
	private int minSupport = 3;
	private int numFeatures;
	private int maxPerGroup;

	private static final class IteratorAdapter implements
			Iterator<Pair<List<Integer>, Long>>
	{
		private final Iterator<Pair<ArrayList<Integer>, Long>> innerIter;

		private IteratorAdapter(
				Iterator<Pair<ArrayList<Integer>, Long>> transactionIter)
		{
			innerIter = transactionIter;
		}

		@Override
		public boolean hasNext()
		{
			return innerIter.hasNext();
		}

		@Override
		public Pair<List<Integer>, Long> next()
		{
			Pair<ArrayList<Integer>, Long> innerNext = innerIter.next();
			return new Pair<List<Integer>, Long>(innerNext.getFirst(),
					innerNext.getSecond());
		}

		@Override
		public void remove()
		{
			throw new UnsupportedOperationException();
		}
	}

	@Override
	protected void reduce(IntWritable key, Iterable<TransactionTree> values,
			Context context) throws IOException
	{
		TransactionTree cTree = new TransactionTree();
		for (TransactionTree tr : values)
		{
			for (Pair<ArrayList<Integer>, Long> p : tr)
			{
				cTree.addPattern(p.getFirst(), p.getSecond());
			}
		}

		List<Pair<Integer, Long>> localFList = Lists.newArrayList();
		for (Entry<Integer, MutableLong> fItem : cTree.generateFList()
				.entrySet())
		{
			localFList.add(new Pair<Integer, Long>(fItem.getKey(), fItem
					.getValue().toLong()));
		}

		Collections.sort(localFList,
				new CountDescendingPairComparator<Integer, Long>());

		FPGrowth<Integer> fpGrowth = new FPGrowth<Integer>();
		fpGrowth.generateTopKFrequentPatterns(
				new IteratorAdapter(cTree.iterator()),
				localFList,
				minSupport,
				maxHeapSize,
				Sets.newHashSet(PFPGrowth.getGroupMembers(key.get(),
						maxPerGroup, numFeatures)),
				new IntegerStringOutputConverter(
						new ContextWriteOutputCollector<IntWritable, TransactionTree, Text, TopKStringPatterns>(
								context), featureReverseMap),
				new ContextStatusUpdater<IntWritable, TransactionTree, Text, TopKStringPatterns>(
						context));

	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException
	{

		super.setup(context);
		Parameters params = new Parameters(context.getConfiguration().get(
				PFPGrowth.PFP_PARAMETERS, ""));

		for (Pair<String, Long> e : PFPGrowth.readFList(context
				.getConfiguration()))
		{
			featureReverseMap.add(e.getFirst());
			freqList.add(e.getSecond());
		}

		maxHeapSize = Integer
				.valueOf(params.get(PFPGrowth.MAX_HEAP_SIZE, "50"));
		minSupport = Integer.valueOf(params.get(PFPGrowth.MIN_SUPPORT, "3"));

		maxPerGroup = params.getInt(PFPGrowth.MAX_PER_GROUP, 5);
		numFeatures = featureReverseMap.size();
	}
}

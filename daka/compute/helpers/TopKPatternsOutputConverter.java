package daka.compute.helpers;

import daka.compute.fpgrowth.FPHeap;
import daka.compute.fpgrowth.Pattern;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import com.google.common.collect.Lists;

import org.apache.hadoop.mapred.OutputCollector;

/**
 * An output converter which converts the output patterns and collects them in FPHeap
 * 
 * @param <A>
 */
public final class TopKPatternsOutputConverter<A extends Comparable<? super A>>
		implements OutputCollector<Integer, FPHeap>
{

	private final OutputCollector<A, List<Pair<List<A>, Long>>> collector;

	private final Map<Integer, A> reverseMapping;

	public TopKPatternsOutputConverter(
			OutputCollector<A, List<Pair<List<A>, Long>>> collector,
			Map<Integer, A> reverseMapping)
	{
		this.collector = collector;
		this.reverseMapping = reverseMapping;
	}

	@Override
	public void collect(Integer key, FPHeap value) throws IOException
	{
		List<Pair<List<A>, Long>> perAttributePatterns = Lists.newArrayList();
		PriorityQueue<Pattern> t = value.getHeap();
		while (!t.isEmpty())
		{
			Pattern itemSet = t.poll();
			List<A> frequentPattern = Lists.newArrayList();
			for (int j = 0; j < itemSet.length(); j++)
			{
				frequentPattern
						.add(reverseMapping.get(itemSet.getPattern()[j]));
			}
			Collections.sort(frequentPattern);

			Pair<List<A>, Long> returnItemSet = new Pair<List<A>, Long>(
					frequentPattern, itemSet.support());
			perAttributePatterns.add(returnItemSet);
		}
		Collections.reverse(perAttributePatterns);

		collector.collect(reverseMapping.get(key), perAttributePatterns);
	}
}

package daka.fpgrowth.parallel;

import java.util.Iterator;
import java.util.ArrayList;

import com.google.common.collect.AbstractIterator;
import daka.helpers.Pair;

/**
 * Iterates over multiple transaction trees to produce a single iterator of
 * transactions
 */
public final class MultiTransactionTreeIterator extends
		AbstractIterator<ArrayList<Integer>>
{

	private final Iterator<Pair<ArrayList<Integer>, Long>> pIterator;
	private ArrayList<Integer> current;
	private long currentMaxCount;
	private long currentCount;

	public MultiTransactionTreeIterator(
			Iterator<Pair<ArrayList<Integer>, Long>> iterator)
	{
		this.pIterator = iterator;
	}

	@Override
	protected ArrayList<Integer> computeNext()
	{
		if (currentCount >= currentMaxCount)
		{
			if (pIterator.hasNext())
			{
				Pair<ArrayList<Integer>, Long> nextValue = pIterator.next();
				current = nextValue.getFirst();
				currentMaxCount = nextValue.getSecond();
				currentCount = 0;
			} else
			{
				return endOfData();
			}
		}
		currentCount++;
		return current;
	}

}

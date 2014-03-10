package daka.fpgrowth;

import java.util.PriorityQueue;
import java.util.Set;
import java.util.Map;

import com.google.common.collect.Sets;
import com.google.common.collect.Maps;


/** keeps top K Attributes in a TreeSet */
public final class FPHeap
{
	private int count;
	private Pattern least;
	private final int maxSize;
	private final boolean subPatternCheck;
	private final Map<Long, Set<Pattern>> patternIndex;
	private final PriorityQueue<Pattern> queue;

	public FPHeap(int numResults, boolean subPatternCheck)
	{
		maxSize = numResults;
		queue = new PriorityQueue<Pattern>(maxSize);
		this.subPatternCheck = subPatternCheck;
		patternIndex = Maps.newHashMap();
		for (Pattern p : queue)
		{
			Long index = p.support();
			Set<Pattern> patternList;
			if (!patternIndex.containsKey(index))
			{
				patternList = Sets.newHashSet();
				patternIndex.put(index, patternList);
			}
			patternList = patternIndex.get(index);
			patternList.add(p);

		}
	}

	public boolean addable(long support)
	{
		return count < maxSize || least.support() <= support;
	}

	public PriorityQueue<Pattern> getHeap()
	{
		if (subPatternCheck)
		{
			PriorityQueue<Pattern> ret = new PriorityQueue<Pattern>(maxSize);
			for (Pattern p : queue)
			{
				if (patternIndex.get(p.support()).contains(p))
				{
					ret.add(p);
				}
			}
			return ret;
		}
		return queue;
	}

	public void addAll(FPHeap patterns, int attribute, long attributeSupport)
	{
		for (Pattern pattern : patterns.getHeap())
		{
			long support = Math.min(attributeSupport, pattern.support());
			if (this.addable(support))
			{
				pattern.add(attribute, support);
				this.insert(pattern);
			}
		}
	}

	public void insert(Pattern frequentPattern)
	{
		if (frequentPattern.length() == 0)
		{
			return;
		}

		if (count == maxSize)
		{
			if (frequentPattern.compareTo(least) > 0
					&& addPattern(frequentPattern))
			{
				Pattern evictedItem = queue.poll();
				least = queue.peek();
				if (subPatternCheck)
				{
					patternIndex.get(evictedItem.support()).remove(evictedItem);
				}
			}
		} else
		{
			if (addPattern(frequentPattern))
			{
				count++;
				if (least == null)
				{
					least = frequentPattern;
				} else
				{
					if (least.compareTo(frequentPattern) < 0)
					{
						least = frequentPattern;
					}
				}
			}
		}
	}

	public int count()
	{
		return count;
	}

	public boolean isFull()
	{
		return count == maxSize;
	}

	public long leastSupport()
	{
		if (least == null)
		{
			return 0;
		}
		return least.support();
	}

	private boolean addPattern(Pattern frequentPattern)
	{
		if (subPatternCheck)
		{
			Long index = frequentPattern.support();
			if (patternIndex.containsKey(index))
			{
				Set<Pattern> indexSet = patternIndex.get(index);
				boolean replace = false;
				Pattern replacablePattern = null;
				for (Pattern p : indexSet)
				{
					if (frequentPattern.isSubPatternOf(p))
					{
						return false;
					} else if (p.isSubPatternOf(frequentPattern))
					{
						replace = true;
						replacablePattern = p;
						break;
					}
				}
				if (replace)
				{
					indexSet.remove(replacablePattern);
					if (!indexSet.contains(frequentPattern)
							&& queue.add(frequentPattern))
					{
						indexSet.add(frequentPattern);
					}
					return false;
				}
				queue.add(frequentPattern);
				indexSet.add(frequentPattern);
			} else
			{
				queue.add(frequentPattern);
				Set<Pattern> patternList;
				if (!patternIndex.containsKey(index))
				{
					patternList = Sets.newHashSet();
					patternIndex.put(index, patternList);
				}
				patternList = patternIndex.get(index);
				patternList.add(frequentPattern);
			}
		} else
		{
			queue.add(frequentPattern);
		}
		return true;
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder("FreqPatHeap{");
		String sep = "";
		for (Pattern p : getHeap())
		{
			sb.append(sep).append(p);
			sep = ", ";
		}
		return sb.toString();
	}
}

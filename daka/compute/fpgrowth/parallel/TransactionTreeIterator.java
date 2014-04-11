package daka.compute.fpgrowth.parallel;

import daka.compute.helpers.Pair;

import java.util.Iterator;
import java.util.Stack;
import java.util.ArrayList;

import com.google.common.collect.AbstractIterator;

/**
 * Generates a List of transactions view of Transaction Tree by doing Depth
 * First Traversal on the tree structure
 */
final class TransactionTreeIterator extends
		AbstractIterator<Pair<ArrayList<Integer>, Long>>
{

	private final Stack<int[]> depth = new Stack<int[]>();
	private final TransactionTree transactionTree;

	TransactionTreeIterator(TransactionTree transactionTree)
	{
		this.transactionTree = transactionTree;
		depth.push(new int[] { 0, -1 });
	}

	@Override
	protected Pair<ArrayList<Integer>, Long> computeNext()
	{

		if (depth.isEmpty())
		{
			return endOfData();
		}

		long sum;
		int childId;
		do
		{
			int[] top = depth.peek();
			while (top[1] + 1 == transactionTree.childCount(top[0]))
			{
				depth.pop();
				top = depth.peek();
			}
			if (depth.isEmpty())
			{
				return endOfData();
			}
			top[1]++;
			childId = transactionTree.childAtIndex(top[0], top[1]);
			depth.push(new int[] { childId, -1 });

			sum = 0;
			for (int i = transactionTree.childCount(childId) - 1; i >= 0; i--)
			{
				sum += transactionTree.count(transactionTree.childAtIndex(
						childId, i));
			}
		} while (sum == transactionTree.count(childId));

		Iterator<int[]> it = depth.iterator();
		it.next();
		ArrayList<Integer> data = new ArrayList<Integer>();
		while (it.hasNext())
		{
			data.add(transactionTree.attribute(it.next()[0]));
		}

		Pair<ArrayList<Integer>, Long> returnable = new Pair<ArrayList<Integer>, Long>(
				data, transactionTree.count(childId) - sum);

		int[] top = depth.peek();
		while (top[1] + 1 == transactionTree.childCount(top[0]))
		{
			depth.pop();
			if (depth.isEmpty())
			{
				break;
			}
			top = depth.peek();
		}
		return returnable;
	}

}

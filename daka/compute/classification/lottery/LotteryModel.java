package daka.compute.classification.lottery;

import daka.compute.classification.Feature;
import daka.compute.classification.Vector;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

class LotteryModel
{
	private Random random;
	private ArrayList<Integer> counts;
	private int total;
	private ArrayList<Feature> labels;

	public LotteryModel(Path path) throws IOException
	{
		random=new Random();
		counts=new ArrayList<Integer>();
		labels=new ArrayList<Feature>();

		FileSystem fs = FileSystem.getLocal(new Configuration());
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
		String line=br.readLine();
		while (line != null){
			String[] tokens=line.split("\t");
			Feature label=Feature.fromString(tokens[0]);
			int count=Integer.parseInt(tokens[1]);

			counts.add(count);
			labels.add(label);
			total+=count;

			line=br.readLine();
		}
	}

	public Feature drawTicket()
	{
		int ticket=random.nextInt(total);
		int seen=0;
		for(int i=0; i<labels.size(); i++)
		{
			seen+=counts.get(i);
			if(ticket<seen)
			{
				return labels.get(i);
			}
		}

		return new Feature("Not Found");
	}
}

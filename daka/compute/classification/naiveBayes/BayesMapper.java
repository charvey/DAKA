package daka.compute.classification.naiveBayes;

import daka.io.CSVParser;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class BayesMapper extends MapReduceBase implements Mapper<Object, Text, Person, LongWritable>
{
	private final static LongWritable one = new LongWritable(1);
	
	/**
	 * we need to count the number of times we have seen a term with a given label and we need to output that
	 * 
	 * @param key The label !!?
	 * @param value the features (all unique) associated with this label
	 * @param output 
	 * @param reporter
	 * @throws IOException
	 * */
	public void map(Object key, Text value, OutputCollector <Person, LongWritable> output, Reporter reporter) throws IOException
	{
		
		String line = value.toString();
		CSVParser parser = new CSVParser();
		String [] values = parser.parseLine(line);
		
		String[] arg = new String[4];
		
		arg[0] = values[33];
		arg[1] = values[25];
		arg[2] = values[40];
		arg[3] = values[45];
		
		
		output.collect(new Person(values), one);	
		//output.collect(new Person(new String[]{values[33], "", "", ""}), one);
		
	}

	
}

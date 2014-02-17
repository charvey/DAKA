public class CSVFieldMapper {
	public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) 
		throws IOException 
	{
		String line = value.toString();
		CSVParser parser = new CSVParser();
		String [] values = parser.parseLine(line);
		for(String s : values)
		{
			String temp = s.trim();
			if(temp.length() > 0)
			{
				output.collect(new Text(temp), new IntWritable(1));
			}
		}
	}
}
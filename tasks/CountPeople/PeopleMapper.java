public static class PeopleMapper extends MapReduceBase implements Mapper<Object, Text, Person, IntWritable> 
{
	public void map(Object key, Text value, OutputCollector<Person, IntWritable> output, Reporter reporter) 
		throws IOException 
	{
		String line = value.toString();
		CSVParser parser = new CSVParser();
		String [] values = parser.parseLine(line);
		output.collect(new Person(values[25], values[40], values[45]), new IntWritable(1));
	}
}
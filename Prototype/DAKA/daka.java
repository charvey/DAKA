import java.util.*;
import java.io.*;

public class daka {
	private static final String HADOOP_HOME="/home/user/Desktop/hadoop-1.2.1";

	public static void main(String[] args){
		Dictionary<String,String> d=parse(args);

		if(d.get("version")!=null){
			System.out.println("prototype");
		} else if(d.get("help")!=null){
			System.out.println("Documentation goes here");
		} else if(d.get("t")!=null){
			System.out.println("Running "+d.get("t")+" on "+d.get("i")+" to "+d.get("o"));

			String hadoopBin=HADOOP_HOME+"/bin/hadoop";

			exec(hadoopBin+" dfs -rmr input");

			exec(hadoopBin+" dfs -rmr output");

			exec(hadoopBin+" dfs -mkdir input");

			exec(hadoopBin+" dfs -put "+d.get("i")+" input/"+d.get("i"));

			exec(hadoopBin+" jar people.jar "+d.get("t")+" input output");

			exec(hadoopBin+" dfs -get output/part-00000 "+d.get("o"));
		}
	}

	private static void exec(String s){
			try{
				System.out.println(s);

				Process p=Runtime.getRuntime().exec(s);
				InputStream in=p.getErrorStream();
				InputStreamReader stream=new InputStreamReader(in);
				BufferedReader reader=new BufferedReader(stream);

				String line;
				while((line=reader.readLine())!=null){
					System.out.println(line);
				}
				reader.close();

				System.out.println(p.waitFor());
			} catch(Exception ex){
				System.out.println("Task failed");
				System.out.println(ex.getMessage());
			}
	}

	private static Dictionary<String,String> parse(String[] args){
		Dictionary<String,String> d=new Hashtable<String,String>();
		for(int i=0; i<args.length; i++){
			String name=null;
			String value=null;
			if(args[i].length()>0 && args[i].charAt(0)=='-'){
				name=args[i].replace("-","");
				if(args[i].length()>1 && args[i].charAt(1)=='-'){
					value="true";
				} else if(args.length>i+1) {
					value=args[i+1];
					i++;
				}
			}
			if(name!=null){
				d.put(name,value);
			}
		}
		return d;
	}
}

package daka.util;

import java.util.*;
import java.io.*;

public class HadoopUtil {
	private static final String hadoopBin="$HADOOP_HOME/bin/hadoop";

	public static void hadoopExec(String s){
		exec(hadoopBin+" "+s);
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
}

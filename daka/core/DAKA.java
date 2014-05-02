package daka.core;

import java.io.*;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.SecureClassLoader;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.hadoop.conf.Configuration;

public class DAKA {
	public static void main(String[] args){
		Dictionary<String,String> d=parse(args);

		if(d.get("version")!=null){
			System.out.println("prototype");
		} else if(d.get("help")!=null){
			System.out.println("Documentation goes here");
		} else if(d.get("t")!=null){
			System.out.println("Running "+d.get("t")+" on "+d.get("i")+" to "+d.get("o"));

			Configuration config=new Configuration();
			config.set("fs.default.name", "hdfs://localhost:9000");
			config.set("mapred.job.tracker", "localhost:9001");
			Task task=locateTask(d.get("t"));
			TaskConfig taskConfig=new TaskConfig(
				d.get("i"),d.get("o"),d,config);

			System.out.println("PreExecute");
			task.PreExecute(taskConfig);
			System.out.println("Execute");
			task.Execute(taskConfig);
			System.out.println("PostExecute");
			task.PostExecute(taskConfig);
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

	private static Task locateTask(String taskName){
		try{
			JarFile jarFile = new JarFile("daka.jar");
			Enumeration e = jarFile.entries();

			URL[] urls = { new URL("jar:file:"+"daka.jar"+"!/") };
			URLClassLoader cl = URLClassLoader.newInstance(urls);

			while (e.hasMoreElements()) {
				JarEntry je = (JarEntry) e.nextElement();
				if(je.isDirectory() || !je.getName().endsWith(".class")){
					continue;
				}

				int startIndex=je.getName().lastIndexOf('/')+1;
				int endIndex=je.getName().lastIndexOf('.');
				String className = je.getName().substring(startIndex,endIndex);
				String classPath = je.getName().substring(0,endIndex).replace('/','.');

				if(className.equals(taskName)){
					Class c = cl.loadClass(classPath);
					if(Task.class.isAssignableFrom(c)){
						Task task=(Task)c.newInstance();
						return task;
					}
				}
			}
		} catch(Exception ex){
			System.err.println(ex.getMessage());
		}

		return null;
	}

}

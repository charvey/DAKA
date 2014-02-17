package daka.core;

import java.util.*;
import java.io.*;

public class DAKA {
	public static void main(String[] args){
		Dictionary<String,String> d=parse(args);

		if(d.get("version")!=null){
			System.out.println("prototype");
		} else if(d.get("help")!=null){
			System.out.println("Documentation goes here");
		} else if(d.get("t")!=null){
			System.out.println("Running "+d.get("t")+" on "+d.get("i")+" to "+d.get("o"));

			Task task=locateTask(d.get("t"));
			TaskConfig config=new TaskConfig(
				d.get("i"),d.get("o"),d);

			task.PreExecute(config);
			task.Execute(config);
			task.PostExecute(config);
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

	private static Task locateTask(String name){
		return null;
/*
		JarFile jarFile = new JarFile(pathToJar);
            Enumeration e = jarFile.entries();

            URL[] urls = { new URL("jar:file:" + pathToJar+"!/") };
            cl = URLClassLoader.newInstance(urls);

            while (e.hasMoreElements()) {
                JarEntry je = (JarEntry) e.nextElement();
                if(je.isDirectory() || !je.getName().endsWith(".class")){
                    continue;
                }
                // -6 because of .class
                String className = je.getName().substring(0,je.getName().length()-6);
                className = className.replace('/', '.');
                Class c = cl.loadClass(className);
	}
*/
	}

}

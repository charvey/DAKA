package daka.util;

import daka.core.Task;

public class TaskLocator {
	public static Task locate(String name){
		return null;
	}

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

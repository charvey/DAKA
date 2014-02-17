package daka.util;

public class HDFSUtil{
	public void rmr(String path){
		HadoopUtil.hadoopExec("dfs -rmr "+path);
	}

	public void mkdir(String path){
		HadoopUtil.hadoopExec("dfs -mkdir "+path);
	}

	public void put(String filePath, String destPath){
		HadoopUtil.hadoopExec("dfs -put "+filePath+" "+destPath);
	}

	public void get(String hdfsPath, String destPath){
		HadoopUtil.hadoopExec("dfs -get "+hdfsPath+" "+destPath);
	}
}

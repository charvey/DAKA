package daka.io;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

public class FileOutput{
	private Path hdfsPath;
	private Path destPath;
	private Configuration config;

	public FileOutput(String hdfsPath, String destPath, Configuration config){
		this.hdfsPath=new Path(hdfsPath);
		this.destPath=new Path(destPath);
		this.config=config;
	}

	public void Update() throws IOException{
		boolean copy=false;

		FileSystem hdfsFS=FileSystem.get(config);
		FileStatus hdfsStatus=hdfsFS.getFileStatus(hdfsPath);

		FileSystem destFS=FileSystem.getLocal(config);
		if(!destFS.exists(destPath)){
			copy=true;
		} else {
			FileStatus destStatus=destFS.getFileStatus(destPath);
			copy=hdfsStatus.getModificationTime()>destStatus.getModificationTime();
		}

		if(copy){
			System.out.println("Moving "+hdfsPath+" to "+destPath);
			FileUtil.copy(hdfsFS,hdfsPath,destFS,destPath,false,true,config);
		}
	}
}

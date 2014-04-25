package daka.io;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

public class FileInput{
	private Path filePath;
	private Path destPath;
	private Configuration config;

	public FileInput(String filePath, String destPath, Configuration config){
		this.filePath=new Path(filePath);
		this.destPath=new Path(destPath);
		this.config=config;
	}

	public void Update() throws IOException {
		FileSystem fileFS=FileSystem.getLocal(config);
		FileStatus fileStatus=fileFS.getFileStatus(filePath);
		FileSystem destFS=FileSystem.get(config);
		FileStatus destStatus=destFS.getFileStatus(destPath);

		if(fileStatus.getModificationTime()>destStatus.getModificationTime()){
			FileUtil.copy(fileFS,filePath,destFS,destPath,false,true,config);
		}
	}
}

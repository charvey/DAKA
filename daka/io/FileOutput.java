package daka.io;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

public class FileOutput{
	private String hdfsPath;
	private String destPath;
	private Configuration config;

	public FileOutput(String hdfsPath, String destPath, Configuration config){
		this.hdfsPath=hdfsPath;
		this.destPath=destPath;
		this.config=config;
	}

	public void Update() throws IOException{
		Date hdfsDate=new Date();
		Date destDate=new Date(0);

		if(hdfsDate.compareTo(destDate)>0){
			FileUtil.copy(
				FileSystem.get(config), new Path(hdfsPath),
				FileSystem.getLocal(config), new Path(destPath),
				false,true,config
			);
		}
	}
}

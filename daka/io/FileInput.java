package daka.io;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

public class FileInput{
	private String filePath;
	private String destPath;
	private Configuration config;

	public FileInput(String filePath, String destPath, Configuration config){
		this.filePath=filePath;
		this.destPath=destPath;
		this.config=config;
	}

	public void Update() throws IOException {
		Date fileDate=new Date();
		Date destDate=new Date(0);

		if(fileDate.compareTo(destDate)>0){
			FileUtil.copy(
				FileSystem.getLocal(config), new Path(filePath),
				FileSystem.get(config), new Path(destPath),
				false,true,config
			);
		}
	}
}

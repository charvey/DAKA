package daka.io;

import daka.util.FileUtil;

import java.util.Date;

public class FileOutput{
	private String hdfsPath;
	private String destPath;

	public FileOutput(String hdfsPath, String destPath){
		this.hdfsPath=hdfsPath;
		this.destPath=destPath;
	}

	public void Update(){
		Date hdfsDate=new Date();
		Date destDate=FileUtil.lastModified(this.destPath);

		if(hdfsDate.compareTo(destDate)>0){
			//remove dest
			//get file
		}
	}
}

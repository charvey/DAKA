package daka.io;

import daka.util.FileUtil;

import java.util.Date;

public class FileInput{
	private String filePath;
	private String destPath;

	public FileInput(String filePath, String destPath){
		this.filePath=filePath;
		this.destPath=destPath;
	}

	public void Update(){
		Date fileDate=FileUtil.lastModified(this.filePath);
		Date destDate=new Date(0);

		if(fileDate.compareTo(destDate)>0){
			//remove dest
			//put file
		}
	}
}

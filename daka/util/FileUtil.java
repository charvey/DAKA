package daka.util;

import java.io.File;
import java.util.Date;

public class FileUtil {
	public static Date lastModified(String filePath){
		File file=new File(filePath);
		long modified=file.lastModified();
		Date fileDate=new Date(modified);
		return fileDate;
	}
}

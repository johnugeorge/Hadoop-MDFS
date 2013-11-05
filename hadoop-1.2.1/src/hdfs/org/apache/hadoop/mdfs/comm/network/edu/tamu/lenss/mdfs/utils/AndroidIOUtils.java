package edu.tamu.lenss.mdfs.utils;

import java.io.File;

//import android.os.Environment;

public class AndroidIOUtils {
	
	/**
	 * Create a File handler to the specified path on SD Card
	 * @param path
	 * @return
	 */
	public static File getExternalFile(String path) {
		//return new File(Environment.getExternalStorageDirectory(), path);
		return new File("/", path);
	}
	
	public static boolean isExternalStorageAvailable(){
		//return Environment.MEDIA_MOUNTED.equals(Environment.getExternalStorageState());
		return true;
	}
}

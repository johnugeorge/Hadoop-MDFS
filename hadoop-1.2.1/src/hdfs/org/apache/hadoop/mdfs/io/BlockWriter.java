package org.apache.hadoop.mdfs.io;


import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;

import edu.tamu.lenss.mdfs.Constants;




public class BlockWriter{
	FileOutputStream dataOut;
	String src;
	String actualFileName;

	BlockWriter(String actualFileName,long blockId,boolean append) throws FileNotFoundException,IOException{
		this(getBlockLocationInFS(actualFileName,blockId),append);
	}


	BlockWriter(String fileName,boolean append) throws FileNotFoundException,IOException{
		//System.out.println(" Creating BlockWriter for fileName "+fileName+" append "+append);
		src=fileName;
		File f = new File(fileName);

		if(!f.exists()){
			f.getParentFile().mkdirs();
			f.createNewFile();
		}

		dataOut = new FileOutputStream(fileName,append);
		System.out.println(" Creating BlockWriter for fileName "+fileName+" append "+append+" length "+f.length());

	}


	public void writeBuffer(byte[] buffer,int offset,int length) throws IOException{
		//System.out.println("  writing buffer offset "+ offset+" length "+length + " src "+src);
		dataOut.write(buffer,offset,length);
	}

	public void close() throws IOException{
		System.out.println(" FileOutputStream is closed for fileName "+src);
		dataOut.close();
	}

	public static String getBlockLocationInFS(String actualFileName,long blockId){
		//System.out.println(" Temporary  Data value "+ Constants.MDFS_HADOOP_DATA_DIR);
		String blockLoc= Constants.MDFS_HADOOP_DATA_DIR+actualFileName+"/Blocks/Block-"+ (new Long(blockId)).toString();
		return blockLoc;
	}


}



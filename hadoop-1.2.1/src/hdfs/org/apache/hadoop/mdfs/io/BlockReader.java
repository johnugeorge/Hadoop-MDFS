package org.apache.hadoop.mdfs.io;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;




public class BlockReader{
	FileInputStream dataIn;
	String src;

	BlockReader(long blockId) throws FileNotFoundException,IOException{
		this(getBlockLocationInFS(blockId));
	}


	BlockReader(String fileName) throws FileNotFoundException,IOException{
		src=fileName;
		File f = new File(fileName);

		if(!f.exists()){
			System.out.println(" File to be read doesn't exist "+fileName);
		}

		dataIn = new FileInputStream(fileName);
		System.out.println(" Creating BlockReader for fileName "+fileName);

	}


	public void readBuffer(byte[] buffer,int offset,int length) throws IOException{
		System.out.println("  Reading buffer offset "+ offset+" length "+length + " src "+src);

	}

	public void close() throws IOException{
		System.out.println(" FileInputStream is closed for fileName "+src);
		dataIn.close();
	}

	public static String getBlockLocationInFS(long blockId){
		String blockLoc= "/tmp/MDFS/Blocks/Block-"+ (new Long(blockId)).toString();
		return blockLoc;
	}

}



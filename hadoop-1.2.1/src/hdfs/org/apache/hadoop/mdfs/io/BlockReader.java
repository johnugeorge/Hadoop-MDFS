package org.apache.hadoop.mdfs.io;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;


import edu.tamu.lenss.mdfs.Constants;


public class BlockReader{
	FileInputStream dataIn;
	String src;

	BlockReader(String acutalFileName,long blockId) throws FileNotFoundException,IOException{
		this(getBlockLocationInFS(acutalFileName,blockId));
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


	public int readBuffer(byte[] buffer,int offset,int length) throws IOException{
		//System.out.println("  Reading buffer offset "+ offset+" length "+length + " src "+src);
		return dataIn.read(buffer,offset,length);

	}

	public void close() throws IOException{
		System.out.println(" FileInputStream is closed for fileName "+src);
		dataIn.close();
	}

	public long skip(long n) throws IOException{
		System.out.println(" FileInputStream is skipped for fileName "+src);
		return dataIn.skip(n);
	}

	public static String getBlockLocationInFS(String actualFileName,long blockId){
		String tmp=actualFileName+"/Blocks/Block-"+(new Long(blockId)).toString();
		String blockLoc= "tmp/"+Constants.DIR_DECRYPTED+"/Block-"+ (new Long(blockId)).toString();
		blockLoc += "__"+tmp.hashCode();
		System.out.println(" Read  getBlockLocationInFS string "+blockLoc+" acutalFileName "+tmp);
		return blockLoc;
	}


	public static String getBlockWriteLocationInFS(String actualFileName,long blockId){
		String blockLoc= actualFileName+"/Blocks/Block-"+ (new Long(blockId)).toString();
		return blockLoc;
	}
}



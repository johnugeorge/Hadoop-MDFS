package org.apache.hadoop.mdfs.io;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;
import java.lang.InterruptedException;


import edu.tamu.lenss.mdfs.Constants;

import org.apache.hadoop.mdfs.protocol.MDFSNameProtocol;
import org.apache.hadoop.mdfs.protocol.LocatedBlock;
import org.apache.hadoop.mdfs.protocol.LocatedBlocks;

import org.apache.commons.logging.*;


public class BlockReader{
	FileInputStream dataIn;
	String src;
	MDFSNameProtocol namesystem;
	public static final Log LOG = LogFactory.getLog(BlockReader.class);


	BlockReader(MDFSNameProtocol namesystem,String actualFileName,long blockId,long startOffset) throws FileNotFoundException,IOException{
		this(namesystem,blockId,actualFileName,startOffset,getBlockLocationInFS(actualFileName,blockId));
	}


	BlockReader(MDFSNameProtocol namesystem,long blockId,String actualFileName,long startOffset,String fileName) throws FileNotFoundException,IOException{
		this.namesystem=namesystem;
		src=fileName;
		File f = new File(fileName);
		boolean found=false;
		int retry =5;

		if(!f.exists()){
			System.out.println(" File to be read doesn't exist.Hence fetching the block "+fileName);
			LOG.error(" File to be read doesn't exist.Hence fetching the block "+fileName);
		}
		else{
			//while(true){
				LocatedBlocks blocks= namesystem.getBlockLocations(actualFileName, 0,Long.MAX_VALUE);
				for(LocatedBlock b:blocks.getLocatedBlocks()){
					if(b.getBlock().getBlockId() == blockId){
						found=true;
						if(f.length() != b.getBlockSize()){
							System.out.println(" File already exists for read, but lengths mismatch." +fileName);
							LOG.error(" File already exists for read, but lengths mismatch." +fileName);
							System.out.println(" Existing file size "+ f.length() + " Actual file size "+ b.getBlockSize());
							LOG.error(" Existing file size "+ f.length() + " Actual file size "+ b.getBlockSize());
							throw new FileNotFoundException();
						}
						break;
					}
				}
				//if(found)
				//	break;
				//if(found==false && retry <0 ){
				if(found==false ){
					throw new IOException(" Block id "+blockId+" is not found in namespace of file "+actualFileName);
				}
				//try{
				//	Thread.sleep(1000);
				//}
				//catch(InterruptedException e){
				//	System.out.println(" InterruptedException caught");
				//}
				//retry--;
			//}
			System.out.println(" Same file already exists for read  "+fileName);
			LOG.error(" Same file already exists for read  "+fileName);

		}


		dataIn = new FileInputStream(fileName);
		long toSkip = startOffset;
		while (toSkip > 0) {
			long skipped = dataIn.skip(toSkip);
			if (skipped == 0) {
				throw new IOException("Couldn't initialize input stream");
			}
			toSkip -= skipped;
		}
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
		//String blockLoc= "tmp/"+Constants.DIR_DECRYPTED+"/Block-"+ (new Long(blockId)).toString();
		String blockLoc= Constants.DIR_DECRYPTED+"/Block-"+ (new Long(blockId)).toString();
		blockLoc += "__"+tmp.hashCode();
		//System.out.println(" Read  getBlockLocationInFS string "+blockLoc+" acutalFileName "+tmp);
		return blockLoc;
	}


	public static String getBlockWriteLocationInFS(String actualFileName,long blockId){
		String blockLoc= actualFileName+"/Blocks/Block-"+ (new Long(blockId)).toString();
		return blockLoc;
	}
}



package org.apache.hadoop.mdfs.io;


import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.InterruptedException;
import java.io.File;

import edu.tamu.lenss.mdfs.Constants;

import org.apache.hadoop.mdfs.protocol.MDFSDataProtocol; 
import org.apache.hadoop.mdfs.protocol.MDFSNameProtocol; 
import org.apache.hadoop.mdfs.protocol.LocatedBlock;
import org.apache.hadoop.mdfs.protocol.LocatedBlocks;


public class BlockCopier{


	public static void makeAvailableForAppend(MDFSNameProtocol namesystem,MDFSDataProtocol datasystem,String src,long blockId) throws IOException{

		boolean retrieve=false;
		boolean found=false;
		int retry=5;
		String blockLocIfExists=BlockReader.getBlockLocationInFS(src,blockId);
		File f=new File(blockLocIfExists);
		if(f.exists()){
			//while(true){
				LocatedBlocks blocks= namesystem.getBlockLocations(src, 0,Long.MAX_VALUE);
				for(LocatedBlock b:blocks.getLocatedBlocks()){
					if(b.getBlock().getBlockId() == blockId){
						found=true;
						if(f.length() != b.getBlockSize()){
							System.out.println(" File already exists for append, but lengths mismatch." +src);
							System.out.println(" Existing file size "+ f.length() + " Actual file size "+ b.getBlockSize());
							retrieve=true;
						}
						break;
					}
				}
				//if(found)
				//	break;
				//if(found==false && retry <0 ){
				if(found==false){
					throw new IOException(" Block id "+blockId+" is not found in namespace of file "+src);
				}
				//try{
				//	Thread.sleep(1000);
				//}
				//catch(InterruptedException e){
				//	System.out.println(" InterruptedException caught");
				//}

				//retry--;
			//}
			if(retrieve == false){
				System.out.println(" Same file already exists for append "+src +" length "+f.length()+" actualBlockLoc" +blockLocIfExists);
			}
		}
		else{
			System.out.println(" File doesn't exists for append "+src);
		}

		String blockLoc= BlockReader.getBlockWriteLocationInFS(src,blockId);
		if(!f.exists() || retrieve){
			datasystem.retrieveBlock(src,blockLoc,blockId);
		}
		File srcFile= new File(blockLocIfExists);
		if(!srcFile.exists()){
			throw new IOException(" File retrieved is not present in loc "+ blockLocIfExists);
		}
		File destFile=new File(BlockWriter.getBlockLocationInFS(src,blockId));

		copyFile(srcFile,destFile);


	}

	private static void copyFile(File source, File dest) throws IOException {
		InputStream is = null;
		OutputStream os = null;
		try {
			is = new FileInputStream(source);
			os = new FileOutputStream(dest);
			byte[] buffer = new byte[1024];
			int length;
			while ((length = is.read(buffer)) > 0) {
				os.write(buffer, 0, length);
			}
		} finally {
			is.close();
			os.close();
		}
	}



}



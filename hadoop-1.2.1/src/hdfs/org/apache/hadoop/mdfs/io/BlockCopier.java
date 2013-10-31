package org.apache.hadoop.mdfs.io;


import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;

import edu.tamu.lenss.mdfs.Constants;

import org.apache.hadoop.mdfs.protocol.MDFSNameSystem; 


public class BlockCopier{


	public static void makeAvailableForAppend(MDFSNameSystem namesystem,String src,long blockId) throws IOException{


		String blockLocIfExists=BlockReader.getBlockLocationInFS(src,blockId);
		File f=new File(blockLocIfExists);
		if(!f.exists()){
			String blockLoc= BlockReader.getBlockWriteLocationInFS(src,blockId);
			namesystem.retrieveBlock(src,blockLoc,blockId);
			File srcFile= new File(blockLocIfExists);
			if(!srcFile.exists()){
				throw new IOException(" File retrieved is not present in loc "+ blockLocIfExists);
			}
			File destFile=new File(BlockWriter.getBlockLocationInFS(src,blockId));

			copyFile(srcFile,destFile);

		}

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



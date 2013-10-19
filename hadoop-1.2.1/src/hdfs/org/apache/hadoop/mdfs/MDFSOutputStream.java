package org.apache.hadoop.mdfs;


import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.permission.FsPermission;




public class MDFSOutputStream extends OutputStream {

	boolean closed = false;
	private String src;
	final private long blockSize;
	private short blockReplication; // replication factor of file
	private Progressable progress;
	private MDFSNameSystem namesystem;


	MDFSOutputStream(MDFSNameSystem namesystem,String src,int flags,FsPermission permission,boolean createParent, short replication, long blockSize,Progressable progress,int bufferSize) throws IOException{
		this.src = src;
		this.blockSize = blockSize;
		this.blockReplication = replication;
		this.progress = progress;
		this.namesystem = namesystem;

		namesystem.addNewFile(src,flags,createParent,permission,replication,blockSize);
			//addInode
	}

	 @Override
	   public synchronized void write(int b) throws IOException {

	   }

	  @Override
	    public synchronized void write(byte buf[], int off, int len) throws IOException {

	    }


	  @Override
	  public synchronized void flush() throws IOException {

	  }

	  @Override
	  public synchronized void close() throws IOException {

	  }


	  protected void finalize() throws Throwable {

	  }


	  public long getPos() throws IOException {
		  return 0;
	  }
}

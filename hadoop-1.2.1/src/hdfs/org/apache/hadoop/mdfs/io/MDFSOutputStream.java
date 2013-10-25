package org.apache.hadoop.mdfs.io;


import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mdfs.protocol.MDFSNameSystem;
import org.apache.hadoop.mdfs.protocol.LocatedBlocks;
import org.apache.hadoop.mdfs.utils.MountFlags;
import org.apache.hadoop.mdfs.protocol.BlockInfo;
import org.apache.hadoop.mdfs.protocol.LocatedBlock;


import edu.tamu.lenss.mdfs.comm.ServiceHelper;
import adhoc.tcp.TCPConnection;


public class MDFSOutputStream extends OutputStream {

	boolean closed = false;
	private String src;
	final private long blockSize;
	private short blockReplication; // replication factor of file
	private Progressable progress;
	private MDFSNameSystem namesystem;
	private ServiceHelper serviceHelper;
	private TCPConnection tcpConnection;
	private int myNodeId;
	private MDFSFileCreator mdfsFileCreator;
	private LocatedBlock lastBlock;
	private byte buffer[];
	private int bufCount;
	private boolean addNewBlock;
	private BlockWriter writer;




	public MDFSOutputStream(MDFSNameSystem namesystem,String src,int flags,FsPermission permission,boolean createParent, short replication, long blockSize,Progressable progress,int bufferSize) throws IOException{
		this.src = src;
		this.blockSize = blockSize;
		this.blockReplication = replication;
		this.progress = progress;
		this.namesystem = namesystem;
		this.serviceHelper = ServiceHelper.getInstance();
		this.tcpConnection = TCPConnection.getInstance();
		this.myNodeId = serviceHelper.getMyNode().getNodeId();
		this.buffer= new byte[bufferSize];
		this.bufCount=0;
		this.writer=null;
		boolean append = MountFlags.O_APPEND.isSet(flags);
		lastBlock=null;
		addNewBlock=true;

		this.mdfsFileCreator = new MDFSFileCreator(namesystem,src,flags,replication,blockSize,progress,myNodeId);
		System.out.println(" My Node Id "+ myNodeId);
		namesystem.addNewFile(src,flags,createParent,permission,replication,blockSize,myNodeId);

		LocatedBlocks blocks= getBlockLocations(src,0,Long.MAX_VALUE);
		System.out.println(" Total number of blocks " + blocks.getLocatedBlocks().size());
		if(blocks.getLocatedBlocks().size() != 0){
			if(!append){
				throw new IOException(" No blocks present for Append Operation");
			}
			else {
				//lastBlock= ;
			}

		}

	}

	@Override
	public synchronized void write(int b) throws IOException {
		if(addNewBlock == true) {
			lastBlock=namesystem.addNewBlock(src,myNodeId);
			if(writer != null){
				writer.close();
			}
			writer=new BlockWriter(lastBlock.getBlock().getBlockId());
			addNewBlock=false;
		}
		buffer[bufCount++] = (byte)b;
		if(bufCount == buffer.length) {
			flushBuffer();
			bufCount =0;
			addNewBlock=true;
		}

	}

	@Override
	public void write(byte buf[]) throws IOException{
		System.out.println(" Write called 2");
		if(addNewBlock == true) {
			lastBlock=namesystem.addNewBlock(src,myNodeId);
			if(writer != null){
				writer.close();
			}
			writer=new BlockWriter(lastBlock.getBlock().getBlockId());
			addNewBlock=false;
		}
		for(byte b:buf)
			write(b);

	}	


	@Override
	public synchronized void write(byte buf[], int off, int len) throws IOException {
		System.out.println(" Write called with offset "+off+" length "+len);
		if(addNewBlock == true) {
			lastBlock=namesystem.addNewBlock(src,myNodeId);
			if(writer != null){
				writer.close();
			}
			writer=new BlockWriter(lastBlock.getBlock().getBlockId());
			addNewBlock=false;
		}
		byte[] array= new byte[len];
		System.arraycopy(buf,off,array,0,len);
		for(byte b:array)
			write(b);


	}


	@Override
	public synchronized void flush() throws IOException {
		System.out.println("  flush ");
		flushBuffer();

	}

	@Override
	public synchronized void close() throws IOException {
		System.out.println("  close ");
		if(buffer.length != 0 ){
			flushBuffer();
		}
		writer.close();

	}


	protected void finalize() throws Throwable {

	}


	public long getPos() throws IOException {
		System.out.println(" get pos ");
		return 0;
	}


	LocatedBlocks getBlockLocations(String src, long start, long length) throws IOException {

		return namesystem.getBlockLocations(src, start, length);
	}

	private void flushBuffer() throws IOException {
			if(writer== null){
				throw new IOException(" Block writer is empty");
			}
			else{
				writer.writeBuffer(buffer,0,bufCount);
			}
	}

}

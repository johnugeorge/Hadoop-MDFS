package org.apache.hadoop.mdfs.io;


import java.io.IOException;
import java.io.FileNotFoundException;


import org.apache.hadoop.fs.FSInputStream;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mdfs.protocol.MDFSNameProtocol;
import org.apache.hadoop.mdfs.protocol.MDFSDataProtocol;
import org.apache.hadoop.mdfs.protocol.LocatedBlocks;
import org.apache.hadoop.mdfs.utils.MountFlags;
import org.apache.hadoop.mdfs.protocol.BlockInfo;
import org.apache.hadoop.mdfs.protocol.LocatedBlock;
import org.apache.commons.logging.*;



public class MDFSInputStream extends FSInputStream {

	private boolean closed = false;
	private String src;
	final private long blockSize;
	private MDFSNameProtocol namesystem;
	private MDFSDataProtocol datasystem;
	private long fileLength;
	private long bufferSize;
	private LocatedBlocks fileBlocks;
	private long filePos;
	private long currentBlockEnd;
	private LocatedBlock currentBlock;
	private byte[] oneByteBuf = new byte[1];
	private BlockReader blockReader;
	private Configuration conf;
	public static final Log LOG = LogFactory.getLog(MDFSInputStream.class);




	public MDFSInputStream(MDFSNameProtocol namesystem,MDFSDataProtocol datasystem,Configuration conf,String src,long fileLength,long blockSize,int bufLen) throws IOException{
		this.src = src;
		this.blockSize = blockSize;
		this.namesystem = namesystem;
		this.datasystem = datasystem;
		this.fileLength = fileLength;
		this.bufferSize=bufLen;
		this.filePos=0;
		this.currentBlockEnd=-1;
		this.conf=conf;
		this.closed=false;

		fileBlocks= getBlockLocations(src,0,Long.MAX_VALUE);
		System.out.println(" Total number of blocks " + fileBlocks.getLocatedBlocks().size()+" getFileLength() "+getFileLength());
		System.out.println(" blockSize "+blockSize+" bufferSize "+bufferSize);
		if(fileLength != getFileLength())
			throw new IOException(" FileLength mismatch. write happened after open "+ getFileLength()+ "fileLength"+ fileLength);

	}


	protected void finalize() throws Throwable {

	}

	@Override
	public synchronized long getPos() throws IOException {
		return filePos;
	}


	@Override
	public synchronized int available() throws IOException {
		System.out.println(" InputStream ReadBuffer available");
		if (closed) {
			throw new IOException("Stream closed");
		}
		return (int) (getFileLength() - filePos);
	}


	@Override
	public long skip(long n) throws IOException {
		System.out.println(" InputStream ReadBuffer skip "+n);
		if ( n > 0 ) {
			long curPos = getPos();
			long fileLen = getFileLength();
			if( n+curPos > fileLen ) {
				n = fileLen - curPos;
			}
			seek(curPos+n);
			return n;
		}
		return n < 0 ? -1 : 0;
	}

	@Override
	public boolean markSupported() {
		return false;
	}

	@Override
	public void reset() throws IOException {
		throw new IOException("Mark/reset not supported");
	}

	public synchronized boolean seekToNewSource(long targetPos) {
		return false;
	}

	@Override
	public synchronized int read() throws IOException {
		int ret = read(oneByteBuf, 0, 1 );
		return ( ret <= 0 ) ? -1 : (oneByteBuf[0] & 0xff);
	}

	@Override
	public synchronized int read(byte buf[], int off, int len) throws IOException{
		//System.out.println(" InputStream Read called off "+off+" len "+len+" buf len"+buf.length);
		//System.out.println(" Read called with buf len "+buf.length+ "with offset "+off+" length "+len);
		if(filePos >= getFileLength()){
			return -1;
			//throw new IOException(" FilePosition exceeded fileLength");
		}
		else{
			if(filePos >currentBlockEnd){
				blockReader=blockSeekTo(filePos);
			}
			int realLen = (int) Math.min((long) len, (currentBlockEnd - filePos + 1L));
			int result=readBuffer(buf,off,realLen);
			if(result >= 0) {
				filePos += result;
			} else {
				// got a EOS from reader though we expect more data on it.
				throw new IOException("Unexpected EOS from the reader. FilePos "+filePos+" result "+result+" len "+len
					       +" realLen "+realLen+" currentBlockEnd "+currentBlockEnd+" offset "+off   );
			}
			//for(byte b:buf)
			//	 System.out.println(" ReadByte "+(char)b+" FilePos "+filePos);
	   		return result;		

		}

	}

	private synchronized int readBuffer(byte buf[], int off, int len) 
		throws IOException {

		return blockReader.readBuffer(buf, off, len);

	}

	@Override
	public synchronized void seek(long targetPos) throws IOException {
		System.out.println(" InputStream seek "+targetPos);
		if (targetPos > getFileLength()) {
			throw new IOException("Cannot seek after EOF");
		}
		boolean done = false;
		if (filePos <= targetPos && targetPos <= currentBlockEnd) {
			int diff = (int)(targetPos - filePos);
			try {
				filePos += blockReader.skip(diff);
				if (filePos == targetPos) {
					done = true;
				}
			} catch (IOException e) {
				throw new IOException("Exception while seek to " + targetPos +" of " + src);
			}
		}
		if (!done) {
			filePos = targetPos;
			currentBlockEnd = -1;
		}

	}

	@Override
	public void close() throws IOException {
		System.out.println(" InputStream Closed");
		if ( blockReader != null ) {
			        blockReader.close();
		}	
		closed=true;
	}

	LocatedBlocks getBlockLocations(String src, long start, long length) throws IOException {

		return namesystem.getBlockLocations(src, start, length);
	}

	private synchronized long getFileLength() throws IOException{
		fileBlocks= getBlockLocations(src,0,Long.MAX_VALUE);
		return (fileBlocks == null) ? 0 : fileBlocks.getFileLength();
	}



	private synchronized LocatedBlock getBlockAt(long offset,
			boolean updatePosition) throws IOException {
		if(fileBlocks == null) 
			throw new IOException("locatedBlocks is null");
		int targetBlockIdx = fileBlocks.findBlock(offset);
		if (targetBlockIdx < 0) { // block is not cached
			throw new IOException(" Target offset doesn't exist "+ offset);
		}
		LocatedBlock blk = fileBlocks.get(targetBlockIdx);
		// update current position
		if (updatePosition) {
			this.filePos = offset;
			this.currentBlockEnd = blk.getStartOffset() + blk.getBlockSize() - 1;
			this.currentBlock = blk;
		}
		return blk;
	}

	private synchronized BlockReader blockSeekTo(long target) throws IOException{
		if(target >= getFileLength()){
			throw new IOException(" FilePosition exceeded fileLength");
		}

		if ( blockReader != null ) {
			        blockReader.close(); 
		}
		LocatedBlock targetBlock = getBlockAt(target, true);
		if(target != filePos)
			throw new IOException("Wrong postion " + filePos + " expect " + target);
		long offsetIntoBlock = target - targetBlock.getStartOffset();
		long blockId = targetBlock.getBlock().getBlockId();
		String blockLoc= BlockReader.getBlockWriteLocationInFS(src,blockId);
		System.out.println(" BlockLocation  of block "+blockId +" is "+ blockLoc);
		LOG.error(" BlockLocation  of block "+blockId +" is "+ blockLoc);
		System.out.println(" OffsetIntoBlock "+offsetIntoBlock+" target "+target+" filePos "+filePos);
		LOG.error(" OffsetIntoBlock "+offsetIntoBlock+" target "+target+" filePos "+filePos);
		
		try{
			LOG.error("First Attempt: To Read File"+src+" blockId "+blockId+" blockLoc "+blockLoc);
			blockReader=new BlockReader(namesystem,src,blockId,offsetIntoBlock);
		}
		catch(FileNotFoundException e){

			System.out.println(" Retrieving file from network as file is not present Locally");
			LOG.error("Second Attempt: Retrieving file from network as file is not present Locally"+src+" blockId "+blockId+" blockLoc "+blockLoc);
			datasystem.retrieveBlock(src,blockLoc,blockId);
			LOG.error("Second Attempt: BlockRetrieved"+src+" blockId "+blockId+" blockLoc "+blockLoc);
			blockReader=new BlockReader(namesystem,src,blockId,offsetIntoBlock);
			LOG.error("Block Read Successfully"+src+" blockId "+blockId+" blockLoc "+blockLoc);

		}
		return blockReader;	
		

	}


}


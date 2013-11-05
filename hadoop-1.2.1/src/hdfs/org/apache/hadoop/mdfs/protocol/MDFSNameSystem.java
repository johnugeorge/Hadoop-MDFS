package org.apache.hadoop.mdfs.protocol;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.net.InetSocketAddress;
import java.net.URI;




import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.mdfs.utils.DFSUtil;
import org.apache.hadoop.mdfs.utils.MountFlags;
import org.apache.hadoop.mdfs.protocol.Block;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.net.NetUtils;




import edu.tamu.lenss.mdfs.comm.ServiceHelper;
import edu.tamu.lenss.mdfs.models.DeleteFile;
import org.apache.hadoop.mdfs.io.BlockReader;


public class MDFSNameSystem implements MDFSProtocol{

	private static MDFSNameSystem instance = null;
	private MDFSDirectory mdfsDir;

	private ServiceHelper serviceHelper;
	private int myNodeId;
	private ListOfBlocksOperation ll;
	private MDFSCommunicator commThread;
	private boolean newThreadforMDFSCommunicator = false;//whether to start a new Thread for MDFS communicator
	private Server server;
	private InetSocketAddress serverAddress = null;

	public static final int DEFAULT_PORT = 8020;


	static{
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
	}

	public long getProtocolVersion(String protocol, 
			long clientVersion) throws IOException {
		return MDFSProtocol.versionID;
	}

	MDFSNameSystem(Configuration conf){
		if (conf == null)
			conf = new Configuration();
		mdfsDir= new MDFSDirectory(this,conf);
		this.serviceHelper = ServiceHelper.getInstance();
		this.myNodeId = serviceHelper.getMyNode().getNodeId();
		ll=new ListOfBlocksOperation();
		commThread=new MDFSCommunicator(ll,newThreadforMDFSCommunicator);
		System.out.println(" My Node Id "+ myNodeId);
		
		//TODO
		int handlerCount =10;
		InetSocketAddress socAddr = MDFSNameSystem.getAddress(conf);
		try{
			this.server = RPC.getServer(this, socAddr.getHostName(),
					socAddr.getPort(), handlerCount, false, conf, null);
			this.server.start();  //start RPC server   
			this.serverAddress = this.server.getListenerAddress(); 
			FileSystem.setDefaultUri(conf, getUri(serverAddress));
			System.out.println("MDFS up at: " + this.serverAddress);
		}
		catch(IOException e){

			System.out.println("MDFS protocol not started due to IOExceptiion");
		}



	}


	public static InetSocketAddress getAddress(Configuration conf) {
		//TODO
		String addr = conf.get("dfs.namenode.rpc-address");
		if (addr == null || addr.isEmpty()) {
			return getAddress(FileSystem.getDefaultUri(conf).toString());
		}
		return getAddress(addr);
	}

	public static InetSocketAddress getAddress(String address) {
		return NetUtils.createSocketAddr(address, DEFAULT_PORT);
	}

	public static URI getUri(InetSocketAddress addr) {
		int port = addr.getPort();
		String portString = port == DEFAULT_PORT ? "" : (":"+port);
		return URI.create("mdfs://"+ addr.getHostName()+portString);
	}

	public static MDFSNameSystem getInstance(Configuration conf) {
		if (instance == null) {
			instance = new MDFSNameSystem(conf);
		}
		return instance;
	}

	public boolean mkdirs(String src, FsPermission permissions,boolean inheritPermission) throws IOException {

		boolean status=true;
		if(mdfsDir.isDir(src)){
			throw new FileAlreadyExistsException(" Directory already exists "+src+" Hence not creating");
		}
		status = mdfsDir.mkdirs(src,permissions,inheritPermission);
		return status;
	}


	public boolean addNewFile(String src,int flags,boolean createParent,FsPermission permission,short replication, long blockSize) throws IOException{
		if (!DFSUtil.isValidName(src)) {
			throw new IOException("Invalid name: " + src);
		}
		boolean pathExists = mdfsDir.exists(src);
		boolean append = MountFlags.O_APPEND.isSet(flags);
		boolean overwrite = MountFlags.O_TRUNCAT.isSet(flags);
		if (pathExists && mdfsDir.isDir(src)) {
			throw new FileAlreadyExistsException("Cannot create "+ src + "; already exists as a directory");
		}
		if(!createParent)
			mdfsDir.verifyParentDir(src);

		MDFSINode myFile = mdfsDir.getFileINode(src);
		if (append) {
			if (myFile == null) {
				throw new FileNotFoundException("failed to append to non-existent path "+ src );
			} else if (myFile.isDirectory()) {
				throw new IOException("failed to append since source is a directory" + src);
			}
		} else if (!mdfsDir.isValidToCreate(src)) {
			if (overwrite) {
				delete(src, false);
			} else {
				throw new IOException("failed to create file " + src);
			}
		}

		if(!append)
		{
			MDFSINode inode =mdfsDir.addNewFile(src,permission,replication,blockSize,myNodeId);
			if(inode == null)
				throw new IOException(" Unable to create INode file ");
		}

		return true;
	}


	public MDFSFileStatus lstat(String src) throws IOException {
		MDFSFileStatus stat= mdfsDir.getFileInfo(src);
		return stat;
	}


	public boolean delete(String src, boolean isDir) throws IOException{

		if(isDir && (!mdfsDir.isDirEmpty(src))){
			throw new IOException(src + " is non empty");
		}
		ArrayList<BlockInfo> collectedBlocks = new ArrayList<BlockInfo>();
		mdfsDir.delete(src,collectedBlocks);
		removeBlocks(src,collectedBlocks);

		return true;

	}

	private void removeBlocks(String src,List<BlockInfo> blocks){

		//handle actual deletion of blocks
		DeleteFile deleteFile = new DeleteFile();
		Iterator<BlockInfo> iterator = blocks.iterator();
		while (iterator.hasNext()) {
			String fileName=BlockReader.getBlockWriteLocationInFS(src,iterator.next().getBlockId());
			System.out.println(" Delete File "+fileName);
			deleteFile.setFile(fileName,fileName.hashCode());
		}
		serviceHelper.deleteFiles(deleteFile);

	}

	public String[] listDir(String src) throws IOException {
		return mdfsDir.listDir(src);
	}

	public boolean rename(String src,String dest) throws IOException {
		return mdfsDir.rename(src,dest);
	}


	public LocatedBlocks getBlockLocations(String src, long start, long length) throws IOException {

		return mdfsDir.getBlockLocations(src, start, length);
	}

	public LocatedBlock addNewBlock(String src) throws IOException {

		return mdfsDir.addBlock(src,myNodeId);
	}

	public void notifyBlockAdded(String src,String actualBlockLoc,long blockId,long bufCount) throws IOException{
		BlockOperation blockOps = new BlockOperation(actualBlockLoc,"CREATE");		
		if(newThreadforMDFSCommunicator){
			//ll.addElem(blockOps);
			ll.addToMaxOneElemList(blockOps);
		}
		else{
			boolean ret=commThread.sendBlockOperation(blockOps);
			if(!ret)
				throw new IOException(" Block Creation Failed");
		}
		mdfsDir.notifyBlockAdded(src,blockId,bufCount);
	}

	public void retrieveBlock(String src,String actualBlockLoc,long blockId) throws IOException{
		BlockOperation blockOps = new BlockOperation(actualBlockLoc,"RETRIEVE");	
		if(newThreadforMDFSCommunicator){
			//ll.addElem(blockOps);
			ll.addToMaxOneElemList(blockOps);
		}
		else{
			boolean ret=commThread.sendBlockOperation(blockOps);
			if(!ret)
				throw new IOException(" Block Retrieval Failed");
		}
	}


	public static void main(String argv[]) throws Exception {
		MDFSNameSystem mdfs = getInstance(null);
		if (mdfs != null)
			mdfs.join();
	}

	public void join() {
		try {
			this.server.join();
		} catch (InterruptedException ie) {
		}
	}

}



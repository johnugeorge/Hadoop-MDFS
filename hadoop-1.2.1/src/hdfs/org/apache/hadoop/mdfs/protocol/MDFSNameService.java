package org.apache.hadoop.mdfs.protocol;

import java.util.Set;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import java.net.InetSocketAddress;
import java.net.URI;




import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SetWritable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.mdfs.utils.DFSUtil;
import org.apache.hadoop.mdfs.utils.MountFlags;
import org.apache.hadoop.mdfs.protocol.Block;
import org.apache.hadoop.mdfs.protocol.LocatedBlocks;
import org.apache.hadoop.mdfs.protocol.LocatedBlock;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.net.NetUtils;





import edu.tamu.lenss.mdfs.MDFSDirectory;
import edu.tamu.lenss.mdfs.models.MDFSFileInfo;


import edu.tamu.lenss.mdfs.models.DeleteFile;
import org.apache.hadoop.mdfs.io.BlockReader;

import adhoc.etc.MyTextUtils;
import adhoc.etc.IOUtilities;

import org.apache.commons.logging.*;


public class MDFSNameService implements MDFSNameProtocol,MDFSDirectoryProtocol{

	private static MDFSNameService instance = null;
	public static final Log LOG = LogFactory.getLog(MDFSNameService.class);

	private org.apache.hadoop.mdfs.protocol.MDFSDirectory mdfsDir;
	private edu.tamu.lenss.mdfs.MDFSDirectory commDir;

	private int myNodeId;
	private ListOfBlocksOperation ll;
	private MDFSCommunicator commThread;
	private boolean newThreadforMDFSCommunicator = false;//whether to start a new Thread for MDFS communicator
	private Server server;
	private Server directoryServer;
	private InetSocketAddress serverAddress = null;
	private InetSocketAddress directoryServerAddress = null;

	public static final int DEFAULT_PORT = 8020;


	static{
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
	}

	public long getProtocolVersion(String protocol, 
			long clientVersion) throws IOException {
		return MDFSNameProtocol.versionID;
	}

	MDFSNameService(Configuration conf){
		if (conf == null)
			conf = new Configuration();
		mdfsDir= new org.apache.hadoop.mdfs.protocol.MDFSDirectory(this,conf);
		commDir =new edu.tamu.lenss.mdfs.MDFSDirectory();
		String localIp = IOUtilities.getLocalIpAddress();
		if(!MyTextUtils.isEmpty(localIp)){
			this.myNodeId = IOUtilities.parseNodeNumber(localIp);
		}else{
			System.out.println(" Error in finding the local IPAddress");
		}

		ll=new ListOfBlocksOperation();
		commThread=new MDFSCommunicator(ll,newThreadforMDFSCommunicator);
		System.out.println(" My Node Id "+ myNodeId);
		
		//TODO
		int handlerCount =1;
		InetSocketAddress socAddr = MDFSNameService.getAddress(conf);
		try{
			this.server = RPC.getServer(this, socAddr.getHostName(),
					socAddr.getPort(), handlerCount, false, conf, null);
			this.server.start();  //start RPC server   
			this.serverAddress = this.server.getListenerAddress(); 
			FileSystem.setDefaultUri(conf, getUri(serverAddress));
			System.out.println("MDFS NameService up at: " + this.serverAddress);
		}
		catch(IOException e){

			System.out.println("MDFS Name Service protocol not started due to IOException");
		}

		InetSocketAddress socDirectoryAddr = MDFSNameService.getDirectoryServiceAddress(conf);
		try{
			this.directoryServer = RPC.getServer(this, socDirectoryAddr.getHostName(),
					socDirectoryAddr.getPort(), handlerCount, false, conf, null);
			this.directoryServer.start();  //start RPC server   
			this.directoryServerAddress = this.directoryServer.getListenerAddress(); 
			FileSystem.setDefaultUri(conf, getUri(directoryServerAddress));
			System.out.println("MDFS DirectoryService up at: " + this.directoryServerAddress);
		}
		catch(IOException e){

			System.out.println("MDFS Directory Service protocol not started due to IOException");
		}


	}


	public static InetSocketAddress getAddress(Configuration conf) {
		//TODO
		String addr = conf.get("mdfs.nameservice.rpc-address");
		if (addr == null || addr.isEmpty()) {
			return getAddress(FileSystem.getDefaultUri(conf).toString());
		}
		return getAddress(addr);
	}

	public static InetSocketAddress getDirectoryServiceAddress(Configuration conf) {
		//TODO
		String addr = conf.get("mdfs.directoryservice.rpc-address");
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

	public static MDFSNameService getInstance(Configuration conf) {
		if (instance == null) {
			instance = new MDFSNameService(conf);
		}
		return instance;
	}

	public boolean mkdirs(String src, FsPermission permissions,boolean inheritPermission) throws IOException {

		boolean status=true;
		if(mdfsDir.isDir(src)){
			System.out.println(" Directory already exists "+src+" Hence not creating");
			return status;
		}
		status = mdfsDir.mkdirs(src,permissions,inheritPermission);
		return status;
	}


	public BlockInfo[] addNewFile(String src,int flags,boolean createParent,FsPermission permission,short replication, long blockSize) throws IOException{
		if (!DFSUtil.isValidName(src)) {
			throw new IOException("Invalid name: " + src);
		}
		boolean pathExists = mdfsDir.exists(src);
		boolean append = MountFlags.O_APPEND.isSet(flags);
		boolean overwrite = MountFlags.O_TRUNCAT.isSet(flags);
		BlockInfo[] collectedBlocks = new BlockInfo[0];


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
				collectedBlocks = delete(src, false);
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

		return collectedBlocks;
	}


	public MDFSFileStatus lstat(String src) throws IOException {
		MDFSFileStatus stat= mdfsDir.getFileInfo(src);
		return stat;
	}


	public BlockInfo[] delete(String src, boolean isDir) throws IOException{

		if(isDir && (!mdfsDir.isDirEmpty(src))){
			throw new IOException(src + " is non empty");
		}
		List<BlockInfo> collectedBlocks = new ArrayList<BlockInfo>();
		mdfsDir.delete(src,collectedBlocks);
		BlockInfo[] blocks= new BlockInfo[collectedBlocks.size()];
		int i=0;
		for(BlockInfo b:collectedBlocks){
			blocks[i++]=b;
		}
		collectedBlocks.clear();
		return blocks;

	}

	public String[] listDir(String src) throws IOException {
		return mdfsDir.listDir(src);
	}

	public boolean rename(String src,String dest) throws IOException {
		System.out.println(" Rename called "+src+" Dest "+dest);
		return mdfsDir.rename(src,dest);
	}


	public LocatedBlocks getBlockLocations(String src, long start, long length) throws IOException {

		return mdfsDir.getBlockLocations(src, start, length);
	}

	public LocatedBlock addNewBlock(String src) throws IOException {
		return mdfsDir.addBlock(src,myNodeId);
	}

	public void notifyBlockAdded(String src,long blockId,long bufCount) throws IOException{

		mdfsDir.notifyBlockAdded(src,blockId,bufCount);

	}


	public static void main(String argv[]) throws Exception {
		MDFSNameService mdfs = getInstance(null);
		if (mdfs != null)
			mdfs.join();
	}

	public void join() {
		try {
			this.server.join();
		} catch (InterruptedException ie) {
		}
	}


	//Directory Service Functions

	public MDFSFileInfo getFileInfo(long fileId){
		return commDir.getFileInfo(fileId);
	}

	public MDFSFileInfo getFileInfo(String fName){
		return commDir.getFileInfo(fName);
	}

	public long getFileIdByName(String name){
		return commDir.getFileIdByName(name);
	}

	public int getStoredKeyIndex(long fileId,int creator){
		return commDir.getStoredKeyIndex(fileId,creator);
	}      

	public int getStoredKeyIndex(String fName,int creator){
		return commDir.getStoredKeyIndex(fName,creator);
	}

	public SetWritable getStoredFileIndex(long fileId,int creator){
		return commDir.getStoredFileIndex(fileId,creator);
	}

	public SetWritable getStoredFileIndex(String fName,int creator){
		return  commDir.getStoredFileIndex(fName,creator);
	}

	public void addFile(MDFSFileInfo file){
		commDir.addFile(file);
	}

	public void removeFile(long fileId){
		commDir.removeFile(fileId);
	}

	public void addKeyFragment(long fileId, int keyIndex,int creator){
		commDir.addKeyFragment(fileId,keyIndex,creator);
	}

	public void addKeyFragment(String fileName, int keyIndex,int creator){
		commDir.addKeyFragment(fileName,keyIndex,creator);
	}

	public void replaceKeyFragment(long src,long dst){
		commDir.replaceKeyFragment(src,dst);
	}
	
	public void replaceFileFragment(long src,long dst){
		commDir.replaceFileFragment(src,dst);
	}

	public void removeKeyFragment(long fileId,int creator){
		commDir.removeKeyFragment(fileId,creator);
	}

	public void removeKeyFragment(String fileName,int creator){
		commDir.removeKeyFragment(fileName,creator);
	}

	public void addFileFragment(long fileId, int fileIndex,int creator){
		commDir.addFileFragment(fileId,fileIndex,creator);
	}

	public void addFileFragment(String fileName, int fileIndex,int creator){
		commDir.addFileFragment(fileName,fileIndex,creator);
	}

	public void addFileFragment(long fileId, SetWritable fileIndex,int creator){
		commDir.addFileFragment(fileId,fileIndex,creator);
	}

	public void addFileFragment(String fileName, SetWritable fileIndex,int creator){
		commDir.addFileFragment(fileName,fileIndex,creator);
	}


	public void removeFileFragment(long fileId,int creator){
		commDir.removeFileFragment(fileId,creator);
	}

	public void removeFileFragment(String fileName,int creator){
		commDir.removeFileFragment(fileName,creator);
	}

	public void addEncryptedFile(long fileId){
		commDir.addEncryptedFile(fileId);
	}

	public void addEncryptedFile(String fileName){
		commDir.addEncryptedFile(fileName);
	}

	public void removeEncryptedFile(long fileId){
		commDir.removeEncryptedFile(fileId);
	}

	public void removeEncryptedFile(String fileName){
		commDir.removeEncryptedFile(fileName);
	}       

	public void addDecryptedFile(long fileId){
		commDir.addDecryptedFile(fileId);
	}

	public void addDecryptedFile(String fileName){
		commDir.addDecryptedFile(fileName);
	}


	public void removeDecryptedFile(long fileId){
		commDir.removeDecryptedFile(fileId);
	}

	public void removeDecryptedFile(String fileName){
		commDir.removeDecryptedFile(fileName);
	}

	public boolean isEncryptedFileCached(long fileId){
		return commDir.isEncryptedFileCached(fileId);
	}
	public boolean isDecryptedFileCached(long fileId){
		return commDir.isDecryptedFileCached(fileId);
	}

	public void clearAll() {
		commDir.clearAll();
	}

	public boolean saveDirectory(){
		return commDir.saveDirectory();
	}

	public void syncLocal(int nodeId){
		commDir.syncLocal(nodeId);
	}

	public MDFSInfoList getFileList(){
		return commDir.getFileList();
	}

}



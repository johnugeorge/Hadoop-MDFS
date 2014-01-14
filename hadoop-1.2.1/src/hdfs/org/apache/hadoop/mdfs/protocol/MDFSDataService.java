package org.apache.hadoop.mdfs.protocol;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
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
import edu.tamu.lenss.mdfs.models.RenameFile;
import org.apache.hadoop.mdfs.io.BlockReader;

import adhoc.etc.IOUtilities;
import adhoc.etc.MyTextUtils;

import org.apache.commons.logging.*;


public class MDFSDataService implements MDFSDataProtocol{

	private static MDFSDataService instance = null;

	public static final Log LOG = LogFactory.getLog(MDFSDataService.class);
	private ServiceHelper serviceHelper;
	private int myNodeId;
	private ListOfBlocksOperation ll;
	private MDFSCommunicator commThread;
	private boolean newThreadforMDFSCommunicator = false;//whether to start a new Thread for MDFS communicator
	private Server server;
	private InetSocketAddress serverAddress = null;

	public static final int DEFAULT_PORT = 8021;


	static{
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
	}

	public long getProtocolVersion(String protocol, 
			long clientVersion) throws IOException {
		return MDFSDataProtocol.versionID;
	}

	MDFSDataService(Configuration conf){
		if (conf == null)
			conf = new Configuration();
		ServiceHelper.setConf(conf);
		ServiceHelper.setStandAloneConf(false);
		this.serviceHelper = ServiceHelper.getInstance();
		this.myNodeId = serviceHelper.getMyNode().getNodeId();
		ll=new ListOfBlocksOperation();
		commThread=new MDFSCommunicator(ll,newThreadforMDFSCommunicator);
		System.out.println(" My Node Id "+ myNodeId);
		
		//TODO
		int handlerCount =1;
		InetSocketAddress socAddr = MDFSDataService.getAddress(IOUtilities.getLocalIpAddress());
		try{
			this.server = RPC.getServer(this, socAddr.getHostName(),
					socAddr.getPort(), handlerCount, false, conf, null);
			this.server.start();  //start RPC server   
			this.serverAddress = this.server.getListenerAddress(); 
			FileSystem.setDefaultUri(conf, getUri(serverAddress));
			System.out.println("MDFS Data Service up at: " + this.serverAddress);
		}
		catch(IOException e){

			System.out.println("MDFS Data protocol not started due to IOException");
		}

	}

	public static InetSocketAddress getAddress(String address) {
		if(address == null)
			System.out.println(" Error in finding the local IPAddress");

		return NetUtils.makeSocketAddr(address, DEFAULT_PORT);
	}

	public static URI getUri(InetSocketAddress addr) {
		int port = addr.getPort();
		String portString = port == DEFAULT_PORT ? "" : (":"+port);
		return URI.create("mdfs://"+ addr.getHostName()+portString);
	}

	public static MDFSDataService getInstance(Configuration conf) {
		if (instance == null) {
			instance = new MDFSDataService(conf);
		}
		return instance;
	}


	public boolean removeBlocks(String src,BlockInfo[] arrayOfBlocks){

		//handle actual deletion of blocks
		DeleteFile deleteFile = new DeleteFile();
		List<BlockInfo> blocks = Arrays.asList(arrayOfBlocks);
		Iterator<BlockInfo> iterator = blocks.iterator();
		while (iterator.hasNext()) {
			String fileName=BlockReader.getBlockWriteLocationInFS(src,iterator.next().getBlockId());
			System.out.println(" Delete File "+fileName);
			deleteFile.setFile(fileName,fileName.hashCode());
		}
		serviceHelper.deleteFiles(deleteFile);
		return true;

	}

	public void notifyBlockAdded(String src,String actualBlockLoc,long blockId,long bufCount) throws IOException{
		LOG.info("MDFSDataService: Adding a new file "+ actualBlockLoc+" src "+src+" blockId "+blockId);
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
		LOG.info("MDFSDataService: New file added"+ actualBlockLoc);
		//mdfsDir.notifyBlockAdded(src,blockId,bufCount);
	}

	public void retrieveBlock(String src,String actualBlockLoc,long blockId) throws IOException{
		LOG.info("MDFSDataService: Retrieving  a  file "+ actualBlockLoc+" src "+src+" blockId "+blockId);
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
		LOG.info("MDFSDataService: Retrieving file done "+ actualBlockLoc+" src "+src+" blockId "+blockId);

	}


	public boolean rename(String src,String dest,LocatedBlocks blocks){
		RenameFile renameFile = new RenameFile();

		List<Long> blockIds= new ArrayList<Long>();
		for(LocatedBlock b:blocks.getLocatedBlocks()){
			blockIds.add(b.getBlock().getBlockId());
		}

		renameFile.setFile(src,dest,blockIds);
		serviceHelper.renameFiles(renameFile);
		return true;

	}


	public static void main(String argv[]) throws Exception {
		MDFSDataService mdfs = getInstance(null);
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



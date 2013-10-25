package org.apache.hadoop.mdfs.io;


import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mdfs.protocol.MDFSNameSystem;

import edu.tamu.lenss.mdfs.comm.ServiceHelper;
import edu.tamu.lenss.mdfs.comm.TopologyHandler.TopologyListener;
import edu.tamu.lenss.mdfs.models.NodeInfo;
import adhoc.tcp.TCPConnection;



public class MDFSFileCreator {


	private String src;
	final private long blockSize;
	private short blockReplication; // replication factor of file
	private Progressable progress;
	private MDFSNameSystem namesystem;
	private ServiceHelper serviceHelper;
	private TCPConnection tcpConnection;
	private int myNodeId;
	private int flags;
	private TopologyListener topologyListener;
	private List<NodeInfo> nodeInfo = new ArrayList<NodeInfo>();



	public MDFSFileCreator(MDFSNameSystem namesystem,String src,int flags,short replication,long blockSize,
						Progressable progress,int myNodeId){
		this.serviceHelper = ServiceHelper.getInstance();
		this.tcpConnection = TCPConnection.getInstance();
		this.namesystem =namesystem;
		this.src=src;
		this.flags=flags;
		this.blockReplication = replication;
		this.blockSize = blockSize;
		this.progress = progress;
		this.myNodeId = myNodeId;
	}	


	public void start() {
		discoverTopology();
		//setUpTimer();
	}

	private void discoverTopology() {
		}


}

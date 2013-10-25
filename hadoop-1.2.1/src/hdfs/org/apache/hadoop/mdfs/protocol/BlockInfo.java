package org.apache.hadoop.mdfs.protocol;


import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.mdfs.protocol.Block;

import edu.tamu.lenss.mdfs.models.NodeInfo;

public class BlockInfo extends Block{
	private MDFSINodeFile          inode;


	private NodeInfo[] locs;

	BlockInfo(Block blk, int replication) {
		super(blk);
		this.locs = new NodeInfo[0];
		this.inode = null;
	}

	BlockInfo(){
		super();
		this.locs = new NodeInfo[0];
		this.inode = null;
	}

	MDFSINodeFile getINode() {
		return inode;
	}

	NodeInfo[] getLocs(){
		return locs;
	}

	void setMDFSINode(MDFSINodeFile inode) {
		this.inode = inode;
	}
}

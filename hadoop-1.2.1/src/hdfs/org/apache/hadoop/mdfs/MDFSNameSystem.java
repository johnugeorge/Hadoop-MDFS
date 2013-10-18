package org.apache.hadoop.mdfs;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.permission.FsPermission;



public class MDFSNameSystem{

	private static MDFSNameSystem instance = null;
	private MDFSDirectory mdfsDir;

	MDFSNameSystem(Configuration conf){
		
	       mdfsDir= new MDFSDirectory(this,conf);

	}

	public static MDFSNameSystem getInstance(Configuration conf) {
		if (instance == null) {
			instance = new MDFSNameSystem(conf);
		}
		return instance;
	}

	public boolean mkdirs(String src, FsPermission permissions,boolean inheritPermission) throws IOException {
		
		boolean status = mdfsDir.mkdirs(src,permissions,inheritPermission);
		return status;
	}

	public boolean addNewFile(String src,int flags,FsPermission permission,short replication, long blockSize){

		return true;
	}


	MDFSFileStatus lstat(String src) throws IOException {
		MDFSFileStatus stat= mdfsDir.getFileInfo(src);
		return stat;
	}


}



package org.apache.hadoop.mdfs;

import java.io.IOException;
import java.net.URI;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.io.OutputStream;
import java.net.InetSocketAddress;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.util.Progressable;

import org.apache.hadoop.mdfs.protocol.MDFSNameService;
import org.apache.hadoop.mdfs.protocol.MDFSDataService;
import org.apache.hadoop.mdfs.protocol.MDFSFileStatus;
import org.apache.hadoop.mdfs.protocol.MDFSNameProtocol;
import org.apache.hadoop.mdfs.protocol.BlockInfo;
import org.apache.hadoop.mdfs.protocol.MDFSDataProtocol;
import org.apache.hadoop.mdfs.protocol.LocatedBlocks;

import org.apache.hadoop.mdfs.io.MDFSOutputStream;
import org.apache.hadoop.mdfs.io.MDFSInputStream;
import org.apache.hadoop.mdfs.utils.DFSUtil;

import org.apache.hadoop.ipc.RPC;
import adhoc.etc.IOUtilities;


class MDFSClient {

	private short defaultReplication;
	private boolean clientRunning;
	private MDFSNameProtocol namesystem;
	private MDFSDataProtocol datasystem;
	private Configuration conf;

	public MDFSClient(Configuration conf) {
	}

	private String pathString(Path path) throws IOException{
		String pathName= path.toUri().getPath();
		if (!DFSUtil.isValidName(pathName)) {
			throw new IOException("Invalid Path in MDFSClient  " + pathName);
		}

		return pathName;

	}

	void initialize(URI uri, Configuration conf) throws IOException {

		clientRunning=true;
		InetSocketAddress nodeAddr = MDFSNameService.getAddress(conf);
		System.out.println(" Going to connect to MDFSNameService ");

		this.namesystem =  (MDFSNameProtocol) 
			RPC.waitForProxy(MDFSNameProtocol.class,
					MDFSNameProtocol.versionID,
					nodeAddr, 
					conf);
		System.out.println(" Connected to MDFSNameService ");
		nodeAddr= MDFSDataService.getAddress(IOUtilities.getLocalIpAddress());
		System.out.println(" Going to connect to MDFSDataService ");

		this.datasystem =  (MDFSDataProtocol) 
			RPC.waitForProxy(MDFSDataProtocol.class,
					MDFSDataProtocol.versionID,
					nodeAddr, 
					conf);
		System.out.println(" Connected to MDFSDataService ");

		this.conf=conf;
	}

	private void checkOpen() throws IOException{
		if(!clientRunning)
			throw(new IOException("FileSystem Closed"));
	}

	OutputStream create(Path path,int flags,FsPermission permission,boolean createParent, short replication, long blockSize,Progressable progress,int bufferSize) throws IOException {
		if (permission == null) {
			permission = FsPermission.getDefault();
		}

		MDFSOutputStream result= new MDFSOutputStream(namesystem,datasystem,conf,pathString(path),flags,permission, createParent,replication,blockSize,progress,bufferSize);
		return result;

	}


	MDFSInputStream open(Path path, long length,long blockSize,int bufferSize) throws IOException {

	
		MDFSInputStream result = new MDFSInputStream(namesystem,datasystem,conf,pathString(path),length,blockSize,bufferSize);

		return result;
	}



	MDFSFileStatus lstat(Path path) throws IOException {
		MDFSFileStatus stat= namesystem.lstat(pathString(path));
		return stat;
	}

	void rmdir(Path path, boolean isDir) throws IOException {

		BlockInfo[] blocks=namesystem.delete(pathString(path),isDir);
		datasystem.removeBlocks(pathString(path),blocks);
	}


	void rename(Path src, Path dst) throws IOException {
		System.out.println(" src "+ pathString(src) +" dst " +pathString(dst) );
		LocatedBlocks blocks = namesystem.getBlockLocations(pathString(src), 0, Long.MAX_VALUE);
		namesystem.rename(pathString(src),pathString(dst));
		datasystem.rename(pathString(src),pathString(dst),blocks);
	}

	String[] listdir(Path path) throws IOException {
		return namesystem.listDir(pathString(path));
	}

	void mkdirs(Path path, FsPermission permission) throws IOException {
		if (permission == null) {
			permission = FsPermission.getDefault();
		}
		namesystem.mkdirs(pathString(path),permission,false);
	}

	void close(int fd) throws IOException {
		clientRunning=false;
	}

	void chmod(Path path, int mode) throws IOException {
	}

	void shutdown() throws IOException {
	}

	short getDefaultReplication() {
		return defaultReplication;
	}

	void setattr(Path path, MDFSFileStatus stat, int mask) throws IOException {
	}

	
}

package org.apache.hadoop.mdfs.protocol;

import java.io.*;

import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.fs.permission.FsPermission;


public interface MDFSProtocol extends VersionedProtocol {

	public static final long versionID = 1L;

	public boolean mkdirs(String src, FsPermission permissions,boolean inheritPermission) throws IOException;

	public boolean addNewFile(String src,int flags,boolean createParent,FsPermission permission,short replication, long blockSize) throws IOException;

	public MDFSFileStatus lstat(String src) throws IOException;

	public boolean delete(String src, boolean isDir) throws IOException;                                                

	public String[] listDir(String src) throws IOException; 



	public boolean rename(String src,String dest) throws IOException;          



	public LocatedBlock addNewBlock(String src) throws IOException;

	public LocatedBlocks getBlockLocations(String src, long start,long length) throws IOException;

	public void notifyBlockAdded(String src,String actualBlockLoc,long blockId,long bufCount) throws IOException;

	public void retrieveBlock(String src,String actualBlockLoc,long blockId) throws IOException;


}


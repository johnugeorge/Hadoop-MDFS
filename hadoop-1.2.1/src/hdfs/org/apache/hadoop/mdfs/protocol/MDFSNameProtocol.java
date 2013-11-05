package org.apache.hadoop.mdfs.protocol;

import java.io.*;
import java.util.ArrayList;

import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.fs.permission.FsPermission;


public interface MDFSNameProtocol extends VersionedProtocol {

	public static final long versionID = 1L;

	public boolean mkdirs(String src, FsPermission permissions,boolean inheritPermission) throws IOException;

	public BlockInfo[] addNewFile(String src,int flags,boolean createParent,FsPermission permission,short replication, long blockSize) throws IOException;

	public MDFSFileStatus lstat(String src) throws IOException;

	public BlockInfo[] delete(String src, boolean isDir) throws IOException;                                                

	public String[] listDir(String src) throws IOException; 

	public boolean rename(String src,String dest) throws IOException;          

	public LocatedBlock addNewBlock(String src) throws IOException;

	public LocatedBlocks getBlockLocations(String src, long start,long length) throws IOException;

	public void notifyBlockAdded(String src,long blockId,long bufCount) throws IOException;



}


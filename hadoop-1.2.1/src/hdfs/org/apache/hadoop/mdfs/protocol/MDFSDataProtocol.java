package org.apache.hadoop.mdfs.protocol;

import java.io.*;
import java.util.List;

import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.fs.permission.FsPermission;


public interface MDFSDataProtocol extends VersionedProtocol {

	public static final long versionID = 1L;

	public boolean removeBlocks(String src,BlockInfo[] blocks);

	public void notifyBlockAdded(String src,String actualBlockLoc,long blockId,long bufCount) throws IOException;

	public void retrieveBlock(String src,String actualBlockLoc,long blockId) throws IOException;

	public boolean rename(String src,String dest,LocatedBlocks blocks);


}


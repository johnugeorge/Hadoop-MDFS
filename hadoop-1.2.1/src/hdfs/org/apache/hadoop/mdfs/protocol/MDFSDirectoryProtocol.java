package org.apache.hadoop.mdfs.protocol;

import java.io.*;
import java.util.ArrayList;
import java.util.Set;


import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.io.SetWritable;
import org.apache.hadoop.fs.permission.FsPermission;


import edu.tamu.lenss.mdfs.models.MDFSFileInfo;


public interface MDFSDirectoryProtocol extends VersionedProtocol {

	public static final long versionID = 1L;

	public MDFSFileInfo getFileInfo(long fileId);

	public MDFSFileInfo getFileInfo(String fName);

	public long getFileIdByName(String name);

	public int getStoredKeyIndex(long fileId,int creator);

	public int getStoredKeyIndex(String fName,int creator);

	public SetWritable getStoredFileIndex(long fileId,int creator);

	public SetWritable getStoredFileIndex(String fName,int creator);

	public void addFile(MDFSFileInfo file);

	public void removeFile(long fileId);

	public void addKeyFragment(long fileId, int keyIndex,int creator);

	public void addKeyFragment(String fileName, int keyIndex,int creator);

	public void replaceKeyFragment(long src,long dst);
	
	public void replaceFileFragment(long src,long dst);

	public void removeKeyFragment(long fileId,int creator);

	public void removeKeyFragment(String fileName,int creator);

	public void addFileFragment(long fileId, int fileIndex,int creator);

	public void addFileFragment(String fileName, int fileIndex,int creator);

	public void addFileFragment(long fileId, SetWritable fileIndex,int creator);

	public void addFileFragment(String fileName, SetWritable fileIndex,int creator);

	public void removeFileFragment(long fileId,int creator);

	public void removeFileFragment(String fileName,int creator);

	public void addEncryptedFile(long fileId);

	public void addEncryptedFile(String fileName);

	public void removeEncryptedFile(long fileId);

	public void removeEncryptedFile(String fileName);

	public void addDecryptedFile(long fileId);

	public void addDecryptedFile(String fileName);

	public void removeDecryptedFile(long fileId);

	public void removeDecryptedFile(String fileName);

	public boolean isEncryptedFileCached(long fileId);

	public boolean isDecryptedFileCached(long fileId);

	public void clearAll() ;

	public boolean saveDirectory();

	public void syncLocal(int nodeId);

	public MDFSInfoList getFileList();


}


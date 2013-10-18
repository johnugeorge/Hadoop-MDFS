package org.apache.hadoop.mdfs;

import java.io.IOException;
import java.io.FileNotFoundException;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.PermissionStatus;



public class MDFSDirectory{


	private MDFSINodeDirectory rootDir;
	private MDFSNameSystem namesystem;
	private Configuration conf;


	public MDFSDirectory (MDFSNameSystem namesystem,Configuration conf){
		this.namesystem = namesystem;
		this.conf=conf;
		rootDir = new MDFSINodeDirectory(MDFSINodeDirectory.ROOT_NAME,
				        new PermissionStatus(null,null,FsPermission.getDefault()));
	}

	public boolean mkdirs(String src, FsPermission permissions,boolean inheritPermission) throws IOException {
		boolean status = true;
		src=normalizePath(src);
		String[] names = MDFSINode.getPathNames(src);
		byte[][] components = MDFSINode.getPathComponents(names);
		MDFSINode[] inodes = new MDFSINode[components.length];

		StringBuilder pathbuilder = new StringBuilder();
		int i = 1;
		for(; i < inodes.length && inodes[i] != null; i++) {
			pathbuilder.append(Path.SEPARATOR + names[i]);
			if (!inodes[i].isDirectory()) {
				throw new FileNotFoundException("Parent path is not a directory: "
						+ pathbuilder);
			}
		}

		for(; i < inodes.length; i++) {
			pathbuilder.append(Path.SEPARATOR + names[i]);
			String cur = pathbuilder.toString();
			createDirectory(inodes, i, components[i], permissions,inheritPermission);
					if (inodes[i] == null) {
						return false;
					}

		}



		return status;
	}

	public void createDirectory(MDFSINode[] inodes, int pos,
			      byte[] name, FsPermission permission,boolean inheritPermission){

		MDFSINode child=new MDFSINodeDirectory(name, permission);
		MDFSINode addedNode = ((MDFSINodeDirectory)inodes[pos-1]).addChild(
				        child, inheritPermission);
		inodes[pos]=addedNode;

	}


	String normalizePath(String src) {
		if (src.length() > 1 && src.endsWith("/")) {
			src = src.substring(0, src.length() - 1);
		}
		return src;
	}

	MDFSFileStatus getFileInfo(String src) {
		String srcs = normalizePath(src);
		synchronized (rootDir) {
			MDFSINode targetNode = rootDir.getNode(srcs);
			if (targetNode == null) {
				return null;
			}
			else {
				return createFileStatus(MDFSFileStatus.EMPTY_NAME,targetNode);
			}
		}
	}

	private static MDFSFileStatus createFileStatus(byte[] path,MDFSINode node) {
		return new MDFSFileStatus(
				node.isDirectory() ? 0 : ((MDFSINodeFile)node).computeContentSummary().getLength(), 
				node.isDirectory(), 
				node.isDirectory() ? 0 : ((MDFSINodeFile)node).getReplication(), 
				node.isDirectory() ? 0 : ((MDFSINodeFile)node).getPreferredBlockSize(),
				node.getLastModifiedTime(),
				node.getAccessTime(),
				node.getFsPermission(),
				node.getUserName(),
				node.getGroupName(),
				path);
	}

}



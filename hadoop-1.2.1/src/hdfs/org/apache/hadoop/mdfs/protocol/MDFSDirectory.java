package org.apache.hadoop.mdfs.protocol;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.mdfs.utils.DFSUtil;
import org.apache.hadoop.mdfs.protocol.Block;

import edu.tamu.lenss.mdfs.models.NodeInfo;



public class MDFSDirectory{


	private static MDFSINodeDirectory rootDir;
	private MDFSNameService namesystem;
	private Configuration conf;
	private long blockId;
	private TreeMap<Block, NodeInfo> treemap;


	public MDFSDirectory (MDFSNameService namesystem,Configuration conf){
		this.namesystem = namesystem;
		this.conf=conf;
		blockId=0;
		rootDir = new MDFSINodeDirectory(MDFSINodeDirectory.ROOT_NAME,
				        new PermissionStatus(null,null,FsPermission.getDefault()));
	}

	public static void printAllChildrenOfSubtrees(){
		System.out.println(" ======================");
		System.out.println(" Printing Complete tree");
		System.out.println(" ======================");
		rootDir.printAllChildrenOfSubtrees();

	}

	public boolean mkdirs(String src, FsPermission permissions,boolean inheritPermission) throws IOException {
		boolean status = true;
		src=normalizePath(src);
		String[] names = MDFSINode.getPathNames(src);
		byte[][] components = MDFSINode.getPathComponents(names);
		MDFSINode[] inodes = new MDFSINode[components.length];

		rootDir.getExistingPathMDFSINodes(components, inodes);

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


	public MDFSINode addNewFile(String src,FsPermission permission,short replication, long blockSize ,int myNodeId) throws IOException{
		if(!mkdirs(new Path(src).getParent().toString(),permission,true))
			return null;
		MDFSINodeFile child = new MDFSINodeFile(permission,0,replication,blockSize);

		byte[][] components = MDFSINode.getPathComponents(src);
		byte[] path = components[components.length-1];
		child.setLocalName(path);
		child.setCreator(myNodeId);
		MDFSINode[] inodes = new MDFSINode[components.length];
		rootDir.getExistingPathMDFSINodes(components, inodes);


		int pos=inodes.length-1;	
		MDFSINode addedNode = ((MDFSINodeDirectory)inodes[pos-1]).addChild(
				        child, true);
		return addedNode;

	}

	String normalizePath(String src) {
		if (src.length() > 1 && src.endsWith("/")) {
			src = src.substring(0, src.length() - 1);
		}
		return src;
	}

	boolean isDir(String src) {
		MDFSINode node = rootDir.getNode(normalizePath(src));
		return node != null && node.isDirectory();
	}


	MDFSFileStatus getFileInfo(String src) {
		String srcs = normalizePath(src);
		MDFSINode targetNode = rootDir.getNode(srcs);
		if (targetNode == null) {
			return null;
		}
		else {
			return createFileStatus(MDFSFileStatus.EMPTY_NAME,targetNode);
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


	public void verifyParentDir(String src) throws IOException{
			Path parent = new Path(src).getParent();
			if (parent != null) {
				MDFSINode[] pathINodes = rootDir.getExistingPathMDFSINodes(parent.toString());
				if (pathINodes[pathINodes.length - 1] == null) {
					throw new FileNotFoundException("Parent directory doesn't exist: "
							+ parent.toString());
				} else if (!pathINodes[pathINodes.length - 1].isDirectory()) {
					throw new FileAlreadyExistsException("Parent path is not a directory: "
							+ parent.toString());
				}
			}
	}


	boolean isValidToCreate(String src) {
		String srcs = normalizePath(src);
		if (srcs.startsWith("/") && 
				!srcs.endsWith("/") && 
				rootDir.getNode(srcs) == null) {
			return true;
		} else {
			return false;
		}
	}

	boolean exists(String src) {
		src = normalizePath(src);
		MDFSINode inode = rootDir.getNode(src);
		if (inode == null) {
			return false;
		}
		return inode.isDirectory()? true: ((MDFSINodeFile)inode).getBlocks() != null;
	}

	MDFSINodeFile getFileINode(String src) {
		MDFSINode inode = rootDir.getNode(src);
		if (inode == null || inode.isDirectory())
			return null;
		return (MDFSINodeFile)inode;
	}

	boolean isDirEmpty(String src) {
		boolean dirNotEmpty = true;
		if (!isDir(src)) {
			return true;
		}
		MDFSINode targetNode = rootDir.getNode(src);
		assert targetNode != null : "should be taken care in isDir() above";
		if (((MDFSINodeDirectory)targetNode).getChildren().size() != 0) {
			dirNotEmpty = false;
		}
		return dirNotEmpty;
	}

	boolean delete(String src, List<BlockInfo>collectedBlocks) throws IOException{

		MDFSINode[] inodes =  rootDir.getExistingPathMDFSINodes(src);
		MDFSINode targetNode = inodes[inodes.length-1];

		if (targetNode == null) { // non-existent src
			throw new FileNotFoundException("failed to remove "+src+" because it does not exist");
		} 
		if (inodes.length == 1) { // src is the root
			throw new IOException("failed to remove " + src +" because the root is not allowed to be deleted");
		} 
		int pos = inodes.length - 1;
		MDFSINode removedNode = ((MDFSINodeDirectory)inodes[pos-1]).removeChild(inodes[pos]);
		if (targetNode == null) {
			throw new IOException(" Not able to delete the src "+src);
		}
		int filesRemoved = targetNode
			          .collectSubtreeBlocksAndClear(collectedBlocks);
		if (filesRemoved <= 0) {
			System.out.println(" No blocks available ");
			return false;
		}
		return true;
	}


	String[] listDir(String src) throws IOException {

		MDFSINode targetNode = rootDir.getNode(src);
		if (targetNode == null)
			return null;

		if (!targetNode.isDirectory()) {
			return null;//file
		}

		MDFSINodeDirectory dirInode = (MDFSINodeDirectory)targetNode; 
		List<MDFSINode> children = dirInode.getChildren();
		String[] childArray = new String[children.size()];
		int start=0;
		for(MDFSINode inode:children){
			childArray[start]=inode.getFileName();
			start++;
		}
		return childArray;


	}

	public boolean rename(String src,String dst) throws IOException {

		MDFSINode[] srcInodes = rootDir.getExistingPathMDFSINodes(src);
		if (srcInodes[srcInodes.length-1] == null) {
			throw new FileNotFoundException("failed to rename " + src + " to " + dst+ " because source does not exist");
		} 
		if (srcInodes.length == 1) {
			 throw new IOException("failed to rename "+src+" to "+dst+ " because source is the root");
		}
		if (dst.equals(src)) {
			throw new FileAlreadyExistsException("failed to rename. source "+src +" is same as destination");
		}
		if (dst.startsWith(src) && dst.charAt(src.length()) == Path.SEPARATOR_CHAR) {
			throw new IOException("failed to rename " + src + " to " + dst+ " because destination starts with src");
		}

		byte[][] dstComponents = MDFSINode.getPathComponents(dst);
		MDFSINode[] dstInodes = new MDFSINode[dstComponents.length];
		rootDir.getExistingPathMDFSINodes(dstComponents, dstInodes);       
		if (dstInodes[dstInodes.length-1] != null) {
			throw new FileAlreadyExistsException("failed to rename " + src + " to " + dst+ " because destination exists");
		}
		if (dstInodes[dstInodes.length-2] == null) {
			throw new FileNotFoundException("failed to rename  because destination's parent does not exist "+  dst);
		}

		MDFSINode dstChild = null;
		MDFSINode srcChild = null;
		String srcChildName = null; 

		int pos=srcInodes.length -1;

		srcChild = ((MDFSINodeDirectory)srcInodes[pos-1]).removeChild(srcInodes[pos]);

		if(srcChild == null)
			throw new IOException(" Cannot rename source "+src +" dest "+ dst);

		srcChildName = srcChild.getFileName();
		srcChild.setLocalName(dstComponents[dstInodes.length-1]);

		pos= dstInodes.length - 1;
		MDFSINode addedNode = ((MDFSINodeDirectory)dstInodes[pos-1]).addChild(
				        srcChild, true);

		//put the child back to src
		if(addedNode == null && srcChild != null){
			 srcChild.setLocalName(srcChildName);
			 pos=srcInodes.length -1;
			 ((MDFSINodeDirectory)srcInodes[pos-1]).addChild(
				        srcChild, true);
			 throw new IOException("Source is not renamed. Henc source is added back ");
		}
		return true;

	}



	LocatedBlocks getBlockLocations(String src, long offset, long length) throws IOException {
		MDFSINodeFile inode = getFileINode(src);
		if(inode == null) {
			return null;
		}
		BlockInfo[] blocks = inode.getBlocks();
		if (blocks == null) {
			return null;
		}
		if (blocks.length == 0) {
			return inode.createLocatedBlocks(new ArrayList<LocatedBlock>(blocks.length));
		}
		List<LocatedBlock> results;
		results = new ArrayList<LocatedBlock>(blocks.length);

		int curBlk = 0;
		long curPos = 0, blkSize = 0;
		int nrBlocks = (blocks[0].getNumBytes() == 0) ? 0 : blocks.length;
		for (curBlk = 0; curBlk < nrBlocks; curBlk++) {
			blkSize = blocks[curBlk].getNumBytes();
			assert blkSize > 0 : "Block of size 0";
			if (curPos + blkSize > offset) {
				break;
			}
			curPos += blkSize;
		}
		if (nrBlocks > 0 && curBlk == nrBlocks)   // offset >= end of file
			return null;

		long endOff = offset + length;
		do {


			LocatedBlock b = new LocatedBlock(blocks[curBlk],curPos);
			results.add(b); 
			curPos += blocks[curBlk].getNumBytes();
			curBlk++;

		} while(curPos < endOff && curBlk < blocks.length );

		return inode.createLocatedBlocks(results);
	}


	public LocatedBlock addBlock(String src,int myNodeId) throws IOException {


		MDFSINodeFile file = getFileINode(src);
		long fileLength = file.computeContentSummary().getLength();
		long blockSize = file.getPreferredBlockSize();
		int replication = (int)file.getReplication();

		MDFSINode[] pathINodes = rootDir.getExistingPathMDFSINodes(src);
		BlockInfo newBlock = allocateBlock(src, pathINodes,myNodeId,replication);
		LocatedBlock b = new LocatedBlock(newBlock, fileLength);

		return b;

	}

	private BlockInfo allocateBlock(String src, MDFSINode[] inodes,int myNodeId,int replication) throws IOException {
		long id = getUniqueBlockId(myNodeId);
		Block b = new Block(id, 0, 0); 
		b.setBlockId(id);
		BlockInfo blockInfo = new BlockInfo(b,replication);
		       
		MDFSINodeFile fileNode = (MDFSINodeFile) inodes[inodes.length-1];
		blockInfo.setMDFSINode(fileNode);
		fileNode.addBlock(blockInfo);

		System.out.println(" New Block created for src "+src+ " with block id "+ id);
		return blockInfo;
	}


	private long getUniqueBlockId(int myNodeId){
		String str = (new Integer(myNodeId)).toString();
		str =str+blockId;
		blockId++;
		return Long.parseLong(str,10);
	}

	public void notifyBlockAdded(String src,long blockId,long bufCount)throws IOException{
		boolean found=false;
		MDFSINode[] inodes = rootDir.getExistingPathMDFSINodes(src);
		MDFSINodeFile fileNode = (MDFSINodeFile) inodes[inodes.length-1];
		BlockInfo[] blocksInfo = fileNode.getBlocks();
		for(BlockInfo b:blocksInfo){
			if(b.getBlockId() == blockId){
				found=true;
				b.setNumBytes(bufCount);
				break;
			}
		}
		if(found == false)
			throw new IOException(" Block is not found with id "+ blockId + " for file "+src);



	}

}



package edu.tamu.lenss.mdfs.models;

import java.util.ArrayList;
import java.util.List;

import adhoc.aodv.pdu.AODVDataContainer;
import adhoc.etc.IOUtilities;

/**
 * Delete one or more files in the Distributed File System. Need to provide both <br>
 * file names and fileId of every file needed to be deleted
 * In situation that a device need to clean up some files without the knowledge of MDFSFileInfo, <br> 
 * both fileNames and fileIds are required to locate the directories.
 * @author Jay
 *
 */
public class RenameFile extends AODVDataContainer {
	private static final long serialVersionUID = 1L;
	private String srcFileName;
	private String destFileName;
	private List<Long> blockIds;
	
	/**
	 * This will be broadcasted to the entire network
	 */
	public RenameFile(){
		super(MDFSPacketType.RENAME_FILE, IOUtilities.parseNodeNumber(IOUtilities.getLocalIpAddress()), 255);
		this.setBroadcast(true);
		this.setMaxHop(5);
	}
	
	/**
	 * If this is sent to a specific node
	 * @param destination
	 * @param broadcast
	 */
	public RenameFile(int destination){
		super(MDFSPacketType.RENAME_FILE, IOUtilities.parseNodeNumber(IOUtilities.getLocalIpAddress()), destination);
		this.setBroadcast(false);
	}
	
	public String getSrcFileName(){
		return srcFileName;
	}

	public String getDestFileName(){
		return destFileName;
	}

	public List<Long> getFileIds() {
		return blockIds;
	}

	/**
	 * fileNames and fileIds need to have the same size
	 * @param fileNames
	 * @param fileIds
	 * @return
	 */

	
	public void setFile(String srcName, String destName ,List<Long> ids){
		srcFileName = srcName;
		destFileName = destName;
		blockIds = ids;

	}

	@Override
	public byte[] toByteArray() {
		return toByteArray(this);
	}

}

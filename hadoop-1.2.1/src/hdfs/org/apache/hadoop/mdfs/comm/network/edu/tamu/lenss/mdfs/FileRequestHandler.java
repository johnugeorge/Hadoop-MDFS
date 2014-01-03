package edu.tamu.lenss.mdfs;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import adhoc.etc.IOUtilities;
import edu.tamu.lenss.mdfs.comm.ServiceHelper;
import edu.tamu.lenss.mdfs.crypto.FragmentInfo.KeyShareInfo;
import edu.tamu.lenss.mdfs.models.FileREP;
import edu.tamu.lenss.mdfs.models.FileREQ;
import edu.tamu.lenss.mdfs.models.MDFSFileInfo;
import edu.tamu.lenss.mdfs.utils.AndroidIOUtils;

import org.apache.hadoop.mdfs.protocol.MDFSDirectoryProtocol;

public class FileRequestHandler {
	private ServiceHelper serviceHelper;
	
	public FileRequestHandler(){
		//serviceHelper = ServiceHelper.getInstance();
	}
	
	/**
	 * Process the FileREQ packet. <br>
	 * Send back the key fragment immediately and append the index of my file fragments. <br>
	 * May need a Thread here	
	 * @param fileReq
	 */
	public void processRequest(FileREQ fileReq){
		serviceHelper = ServiceHelper.getInstance();
		MDFSDirectoryProtocol directory = serviceHelper.getDirectory();
		
		FileREP reply = new FileREP(fileReq.getFileName(), fileReq.getFileCreatedTime(),
				serviceHelper.getMyNode().getNodeId(), fileReq.getSource());
		
		Set<Integer> fileSet = directory.getStoredFileIndex(fileReq.getFileCreatedTime(),serviceHelper.getMyNode().getNodeId()).getItemSet();
		int storedKeyIdx = directory.getStoredKeyIndex(fileReq.getFileCreatedTime(),serviceHelper.getMyNode().getNodeId());
		
		// Reply with whatever fragments I have
		if(fileReq.isAnyAvailable()){
			if(fileSet != null)
				reply.setFileFragIndex(new ArrayList<Integer>(fileSet));
			if(storedKeyIdx >= 0){
				// Retrieve the key fragment
				String dirName = MDFSFileInfo.getDirName(fileReq.getFileName(), fileReq.getFileCreatedTime());
				String fName = MDFSFileInfo.getShortFileName(fileReq.getFileName()) + "__key__" + storedKeyIdx;
				File f = AndroidIOUtils.getExternalFile(Constants.DIR_ROOT + "/" + dirName + "/" + fName);
				KeyShareInfo key = IOUtilities.readObjectFromFile(f, KeyShareInfo.class);
				if(key != null)
					reply.setKeyShare(key);
			}
		}
		// Reply with only the fragments specified in the request
		else{
			// Process FileFrag
			List<Integer> fileFrags = fileReq.getFileFragIndex();
			if(fileFrags != null && fileFrags.size() > 0){
				if(fileSet != null){
					fileFrags.retainAll(fileSet);
					if(!fileFrags.isEmpty())
						reply.setFileFragIndex(fileFrags);
				}
			}
			
			// Process KeyFrag
			List<Integer>keyFrags = fileReq.getKeyFragIndex();
			if(keyFrags != null && keyFrags.size() > 0){
				if(storedKeyIdx >= 0){
					// Retrieve the key fragment
					String dirName = MDFSFileInfo.getDirName(fileReq.getFileName(), fileReq.getFileCreatedTime());
					String fName = MDFSFileInfo.getShortFileName(fileReq.getFileName()) + "__key__" + storedKeyIdx;
					File f = AndroidIOUtils.getExternalFile(Constants.DIR_ROOT + "/" + dirName + "/" + fName);
					KeyShareInfo key = IOUtilities.readObjectFromFile(f, KeyShareInfo.class);
					if(key != null)
						reply.setKeyShare(key);
				}
			}
		}
		
		// Send back the reply
		if(reply.getFileFragIndex() != null || reply.getKeyShare() != null)
			serviceHelper.getMyNode().sendAODVDataContainer(reply);
	}
}

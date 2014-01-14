package edu.tamu.lenss.mdfs;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;

import adhoc.etc.IOUtilities;
import adhoc.etc.Logger;
import adhoc.tcp.TCPReceive.TCPReceiverData;
import edu.tamu.lenss.mdfs.comm.ServiceHelper;
import edu.tamu.lenss.mdfs.crypto.FragmentInfo.KeyShareInfo;
import edu.tamu.lenss.mdfs.models.FileFragPacket;
import edu.tamu.lenss.mdfs.models.KeyFragPacket;
import edu.tamu.lenss.mdfs.models.MDFSFileInfo;
import edu.tamu.lenss.mdfs.utils.AndroidIOUtils;


import org.apache.hadoop.mdfs.protocol.MDFSDirectoryProtocol;

/**
 * All functions in this class are blocking calls. Need to be handled in Thread
 * @author Jay
 *
 */
public class FragExchangeHelper {
	private static final String TAG = FragExchangeHelper.class.getSimpleName();
	//private ExecutorService pool;
	
	public FragExchangeHelper(){
		//pool = Executors.newCachedThreadPool();
	}
	
	public void downloadKeyFragment(final KeyFragPacket keyPacket){
		KeyShareInfo key = keyPacket.getKeyShareInfo();
		String fileDirName = MDFSFileInfo.getDirName(keyPacket.getFileName(), keyPacket.getCreatedTime());

		File tmp0 = AndroidIOUtils.getExternalFile(Constants.DIR_ROOT + "/" + fileDirName);
		int tialCnt = 0;
		while(!tmp0.exists() && !tmp0.mkdirs()){
			// This loop is used to solve race condition between keyFragDownload and fileFragDownload.
			// Both of them compete to create a new file folder
			Thread.yield();
			Logger.v(TAG, "waiting for file handle");
			tmp0 = AndroidIOUtils.getExternalFile(Constants.DIR_ROOT + "/" + fileDirName);
			tialCnt++;
			if(tialCnt >= 600)
				return;
		}
		
		tmp0 = AndroidIOUtils.getExternalFile(Constants.DIR_ROOT + "/" + fileDirName + "/" + 
				key.getFileName()+"__key__" + key.getIndex());
		
		if(tmp0.exists() && tmp0.length() > 0){	// We send KeyFragment multiple times....hacky way to fix things
			Logger.v(TAG, "Returning as file already exists");
			return;
		}
		
		if(IOUtilities.writeObjectToFile(key, tmp0)){
			// Update Directory
			MDFSDirectoryProtocol directory = ServiceHelper.getInstance().getDirectory();
			directory.addKeyFragment(keyPacket.getCreatedTime(), key.getIndex(),ServiceHelper.getInstance().getMyNode().getNodeId());
		}
		else{

			Logger.v(TAG, " Creating key Failure");
		}
	}
	
	private void receiveFileFragment(final TCPReceiverData data, ObjectInputStream ois, FileFragPacket header){
		boolean success=false;
		File tmp0=null;
		try {
			ObjectOutputStream oos = new ObjectOutputStream(data.getDataOutputStream());
			header.setNeedReply(false);
			header.setReady(true);
			oos.writeObject(header);
			
			byte[] buffer = new byte[Constants.TCP_COMM_BUFFER_SIZE];
			
			String fName = header.getFileName();
			fName = fName.substring(0,fName.indexOf("__frag__"));
			String fDirName = MDFSFileInfo.getDirName(fName, header.getCreatedTime());
			tmp0 = AndroidIOUtils.getExternalFile(Constants.DIR_ROOT + "/" +	fDirName );
			int tialCnt = 0;
			while(!tmp0.exists() && !tmp0.mkdirs()){
				// This loop is used to solve race condition between keyFragDownload and fileFragDownload.
				// Both of them compete to create a new file folder
				Thread.yield();
				Logger.v(TAG, "waiting for file handle");
				tmp0 = AndroidIOUtils.getExternalFile(Constants.DIR_ROOT + "/" +	fDirName );
				tialCnt++;
				if(tialCnt >= 600)
					return;
			}
			
			tmp0 = AndroidIOUtils.getExternalFile(Constants.DIR_ROOT + "/" + fDirName + "/" + header.getFileName() );
			System.out.println("  Rcv FileFragment Loc "+(Constants.DIR_ROOT + "/" + fDirName + "/" +  header.getFileName()));
			FileOutputStream fos = new FileOutputStream(tmp0);
			int readLen=0;
			DataInputStream in = data.getDataInputStream();
			while ((readLen = in.read(buffer)) >= 0) {
                fos.write(buffer, 0, readLen);
                //Logger.v(TAG, "read " + readLen + " bytes");
			}
			Logger.v(TAG, "Finish reading data from InputStream");
			
			fos.close();
			data.close();
			oos.close();
			ois.close();
			success = true;
		} catch (StreamCorruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
			if(success && tmp0.length() > 0){ // Hacky way to avoid 0 byte file
				// update directory
				MDFSDirectoryProtocol directory = ServiceHelper.getInstance().getDirectory();
				directory.addFileFragment(header.getCreatedTime(), header.getFragIndex(),ServiceHelper.getInstance().getMyNode().getNodeId());
			}
			else if(tmp0 != null)
				tmp0.delete();
		}
	}
	
	private void sendFileFragment(final TCPReceiverData data, ObjectInputStream ois, FileFragPacket header){
		byte [] mybytearray  = new byte [Constants.TCP_COMM_BUFFER_SIZE];
		String fDirName = MDFSFileInfo.getDirName(header.getFileName(), header.getCreatedTime());
		String fName = MDFSFileInfo.getShortFileName(header.getFileName()) + "__frag__" + header.getFragIndex();
		File fileFrag = AndroidIOUtils.getExternalFile(Constants.DIR_ROOT + "/" + fDirName + "/" + fName );
		System.out.println("  Send FileFragment Loc "+(Constants.DIR_ROOT + "/" + fDirName + "/" + fName));
		if(!fileFrag.exists() || fileFrag.length() < 1){	// Handle the situation that 0 byte file got stored...
			Logger.e(TAG, "File Fragment does not exist");
			fileFrag.delete();
			data.close();
			// Update directory
			MDFSDirectoryProtocol directory = ServiceHelper.getInstance().getDirectory();
			directory.removeFileFragment(header.getCreatedTime(),ServiceHelper.getInstance().getMyNode().getNodeId());
			return;
		}
		
		try {
			int readLen=0;
			FileInputStream fis = new FileInputStream(fileFrag);
			BufferedInputStream bis = new BufferedInputStream(fis);
			DataOutputStream out = data.getDataOutputStream();
			while((readLen=bis.read(mybytearray,0,Constants.TCP_COMM_BUFFER_SIZE))>0){
				out.write(mybytearray,0,readLen);
			}
			Logger.v(TAG, "Finish uploading data to OutStream");
			
			out.close();
			bis.close();
			fis.close();
			ois.close();
			data.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}
	
	public void newIncomingTCP(final TCPReceiverData data){
		try {
			ObjectInputStream ois = new ObjectInputStream(data.getDataInputStream());
			FileFragPacket header = (FileFragPacket)ois.readObject();
			Logger.v(TAG, "Receive file header " + header.getFileName());
			
			if(header.getReqType() == FileFragPacket.REQ_TO_SEND){
				// Download file to the source
				receiveFileFragment(data, ois, header);
			}
			else if(header.getReqType() == FileFragPacket.REQ_TO_RECEIVE){
				// Send file to the source
				sendFileFragment(data, ois, header);
			}
		} catch (StreamCorruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}

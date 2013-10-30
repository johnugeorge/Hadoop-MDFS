package edu.tamu.lenss.mdfs.crypto;

import java.io.File;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import javax.crypto.SecretKey;

import adhoc.aodv.Node;
import adhoc.etc.IOUtilities;
import adhoc.etc.Logger;

import com.tiemens.secretshare.engine.SecretShare;
import com.tiemens.secretshare.engine.SecretShare.ShareInfo;

import edu.tamu.lenss.mdfs.crypto.FragmentInfo.KeyShareInfo;

public class MDFSEncoder {
	private static final String TAG = MDFSEncoder.class.getSimpleName(); 
	private int n1, n2, k1, k2;
	private byte[] plainBytes, encryptedByteFile, rawSecretKey;
	private ArrayList<KeyShareInfo> shares;
	private ArrayList<FragmentInfo> fileFragments;
	private String fileName;
	private long timeStamp;
	//private DataLogger dataLogger;
	
	public MDFSEncoder(File file, int n1, int n2, int k1, int k2,long createdTime){
		this(IOUtilities.fileToByte(file), file.getName(), n1, n2, k1, k2);
		//this.timeStamp = file.lastModified();
		this.timeStamp = createdTime;
		//this.dataLogger = ServiceHelper.getInstance().getDataLogger();
	}
	
	public MDFSEncoder(byte[] bytes, String fName, int n1, int n2, int k1, int k2){
		this.plainBytes = bytes;
		this.fileName = fName;
		this.n1 = n1;
		this.n2 = n2;
		this.k1 = k1;
		this.k2 = k2;
		//this.timeStamp = System.currentTimeMillis();
	}
	
	public boolean encode(){
		Logger.v(TAG," Encoding File "+fileName);
		long startT, endT;
		StringBuilder str = new StringBuilder();
		startT = System.currentTimeMillis();
		if(encryptFile()){
			endT = System.currentTimeMillis();
			
			// Log
			str.append(Node.getInstance().getNodeId() + ", ");			
			str.append("Encryption, ");
			str.append(fileName + ", ");
			str.append(plainBytes.length + ", ");
			str.append(startT + ", ");
			str.append((endT-startT) + ", ");
			str.append("\n");			
			//dataLogger.appendSensorData(LogFileName.TIMES, str.toString());
			
			startT = System.currentTimeMillis();
			if(generateFileShares()){
				endT = System.currentTimeMillis();				
				// Log
				str.delete(0, str.length()-1);
				str.append(Node.getInstance().getNodeId() + ", ");			
				str.append("Encoding, ");
				str.append(fileName + ", ");
				str.append(plainBytes.length + ", ");
				str.append(startT + ", ");
				str.append((endT-startT) + ", ");
				str.append("\n");				
				//dataLogger.appendSensorData(LogFileName.TIMES, str.toString());
				return true;
			}
		}		
		return false;
	}
	
	private boolean encryptFile(){
		Logger.v(TAG," Encrypting File "+fileName);
		// AES Encryption
		MDFSCipher myCipher = MDFSCipher.getInstance();
		// Make Sure the generated key is a positive BigInteger.
		do{
			// Generate the secret key
			SecretKey skey = myCipher.generateSecretKey();
			rawSecretKey = skey.getEncoded();
		}while(new BigInteger(rawSecretKey).signum()<=0);
		
		// Encrypt the plainByteFile
		encryptedByteFile = myCipher.encrypt(plainBytes, rawSecretKey);
		if(encryptedByteFile == null)
			return false;
		BigInteger modulus = SecretShare.getPrimeUsedFor384bitSecretPayload();
		SecretShare.PublicInfo publicInfo = 
            new SecretShare.PublicInfo(n1, k1, modulus, null);
		
		SecretShare secretShare = new SecretShare(publicInfo);
		BigInteger secret;
		try{
			secret = new BigInteger(rawSecretKey);
		} catch(NumberFormatException  e){
			Logger.e(TAG, e.toString());
			return false;
		}
		SecretShare.SplitSecretOutput generate = secretShare.split(secret, new SecureRandom());
		List<ShareInfo> shares0 = generate.getShareInfos();
		if(shares != null)
			shares.clear();
		else
			shares = new ArrayList<KeyShareInfo>(); 
		
		for(ShareInfo info : shares0){
			shares.add(new KeyShareInfo(info, k1, n1, fileName));
		}
		
		return true;
	}
	
	private boolean generateFileShares(){
		byte[][] fragments = new ReedSolomon().encoder(encryptedByteFile, k2, n2);
		if(fileFragments != null)
			fileFragments.clear();
		else
			fileFragments = new ArrayList<FragmentInfo>();
		/*
		 * (String filename, int fragmentType, long filesize, 
			byte[] fragment, int fragmentNumber, int kNumber, int nNumber, long lastModified)
		 */
		int fileSize = encryptedByteFile.length; 
		int type;
		for(int i=0; i < fragments.length; i++){
			if(i < k2)
				type = FragmentInfo.DATA_TYPE;
			else
				type = FragmentInfo.CODING_TYPE;
			
			fileFragments.add(new FragmentInfo(fileName, type, fileSize, fragments[i], i, k2, n2, timeStamp ));
		}
		return (fragments == null ? false : true);
	}

	public int getN1() {
		return n1;
	}

	public int getN2() {
		return n2;
	}

	public int getK1() {
		return k1;
	}

	public int getK2() {
		return k2;
	}

	public byte[] getPlainBytes() {
		return plainBytes;
	}
	
	public byte[] getEncryptedBytes(){
		return encryptedByteFile;
	}

	public byte[] getRawSecretKey() {
		return rawSecretKey;
	}

	public ArrayList<KeyShareInfo> getKeyShares() {
		return shares;
	}
	
	public ArrayList<FragmentInfo> getFileFragments() {
		return fileFragments;
	}
}

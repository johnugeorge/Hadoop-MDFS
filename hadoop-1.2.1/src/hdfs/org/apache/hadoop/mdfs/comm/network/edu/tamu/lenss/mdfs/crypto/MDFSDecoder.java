package edu.tamu.lenss.mdfs.crypto;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import adhoc.aodv.Node;
import adhoc.etc.Logger;

import com.tiemens.secretshare.engine.SecretShare;
import com.tiemens.secretshare.engine.SecretShare.ShareInfo;

import edu.tamu.lenss.mdfs.crypto.FragmentInfo.KeyShareInfo;


public class MDFSDecoder {
	private static final String TAG = MDFSDecoder.class.getSimpleName(); 
	private int n1, n2, k1, k2;	// Can be accesses from shares and fileFragments
	private byte[] plainBytes, encryptedByteFile, rawSecretKey;
	private List<KeyShareInfo> shares;
	private List<FragmentInfo> fileFragments;
	private String fileName;
	//private DataLogger dataLogger;
	
	public MDFSDecoder(List<KeyShareInfo> keys, List<FragmentInfo> files){
		
		// TODO Need to check the validity of ArrayList<KeyShareInfo> and ArrayList<FragmentInfo>.
		// They should come from the same file
		this.shares = keys;
		this.fileFragments = files;
		KeyShareInfo kShare = keys.get(0);
		FragmentInfo fShare = fileFragments.get(0);
		this.n1 = kShare.getN();
		this.k1 = kShare.getK();
		this.n2 = fShare.getN();
		this.k2 = fShare.getK();
		this.fileName = fShare.getFileName();
		//this.dataLogger = ServiceHelper.getInstance().getDataLogger();
	}
	
	/**
	 * Blocking call
	 * @return
	 */
	public boolean decode(){
		boolean success=false;
		try{
			//success = (combineFileFragments() && decryptFile());			
			long startT, endT;
			StringBuilder str = new StringBuilder();
			startT = System.currentTimeMillis();
			if(combineFileFragments()){
				endT = System.currentTimeMillis();
				// Log
				str.append(Node.getInstance().getNodeId() + ", ");			
				str.append("Decryption, ");
				str.append(fileName + ", ");
				//str.append(plainBytes.length + ", ");
				str.append(startT + ", ");
				str.append((endT-startT) + ", ");
				str.append("\n");			
				//dataLogger.appendSensorData(LogFileName.TIMES, str.toString());
				
				startT = System.currentTimeMillis();
				if(decryptFile()){
					endT = System.currentTimeMillis();				
					// Log
					str.delete(0, str.length()-1);
					str.append(Node.getInstance().getNodeId() + ", ");			
					str.append("Decoding, ");
					str.append(fileName + ", ");
					str.append(plainBytes.length + ", ");
					str.append(startT + ", ");
					str.append((endT-startT) + ", ");
					str.append("\n");				
					//dataLogger.appendSensorData(LogFileName.TIMES, str.toString());
					success = true;
				}
			}
			
		} catch(Exception e){
			// There are all kind of exceptions may happen here...Just not sure which one it may be
			e.printStackTrace();
		} 
		return success;	
	}
	
	private boolean decryptFile(){
		// Combine Key Fragments
		BigInteger modulus = SecretShare.getPrimeUsedFor384bitSecretPayload();
		SecretShare.PublicInfo publicInfo = new SecretShare.PublicInfo(n1, k1, modulus, null);
		SecretShare secretShare = new SecretShare(publicInfo);
		List<ShareInfo> shareInfos = new ArrayList<ShareInfo>();
		for(KeyShareInfo info : shares){
			shareInfos.add(info.getShareInfo());
		}
		SecretShare.CombineOutput combine= secretShare.combine(shareInfos);
		rawSecretKey = combine.getSecret().toByteArray();
		
		// Decrypt
		MDFSCipher myCipher = MDFSCipher.getInstance();
		plainBytes = myCipher.decrypt(encryptedByteFile, rawSecretKey);
		
		Logger.v(TAG, plainBytes != null ? "Decrypt Successfully" : "Decrypt Fails");
		return (plainBytes != null ? true : false);
	}
	
	private boolean combineFileFragments(){
		int blocksize = fileFragments.get(0).getFragment().length;
		long filesize = fileFragments.get(0).getFilesize();
		int m = n2-k2;
		
		// Jerasure parameters
		int numerased = 0;
		int[] erased = new int[n2];
		int[] erasures = new int[n2];
		byte[][] data = new byte[k2][blocksize];
		byte[][] coding = new byte[m][blocksize];
		Logger.v(TAG, "Block Size: " + blocksize + " File Size: " + filesize);
		// initialize erased
		for (int i = 0; i < n2; i++) {
			erased[i] = 0;
		}
		
		// initialize data and coding
		for (int i = 0; i < k2; i++) {
			data[i] = null;
		}
		for (int i = 0; i < m; i++) {
			coding[i] = null;
		}
		int index; 
		for(FragmentInfo frag : fileFragments){
			index = frag.getFragmentNumber();
			if(frag.getType() == FragmentInfo.DATA_TYPE){
				data[index] = frag.getFragment();
			}
			else{
				coding[index-k2]=frag.getFragment();
			}
		}

		// process erased and erasures
		for (int i = 0; i < k2; i++) {
			if (data[i] == null) {
				erased[i] = 1;
				erasures[numerased] = i;
				numerased++;
			}
		}
		for (int i = 0; i < m; i++) {
			if (coding[i] == null) {
				erased[k2 + i] = 1;
				erasures[numerased] = k2 + i;
				numerased++;
			}
		}
		
		erasures[numerased] = -1;	// Indicate the end of erasure
		if(numerased > m){
			Logger.e(TAG, "Not Enough Fragments to recover the file");
			return false;
		}
		
		encryptedByteFile = new ReedSolomon().decoder(data, coding,
				erasures, erased, filesize, blocksize, k2, n2);
		
		
		Logger.v(TAG, encryptedByteFile != null ? "Combine Successfully" : "Combine Failed");
		return (encryptedByteFile != null ? true : false);
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

	public byte[] getEncryptedByteFile() {
		return encryptedByteFile;
	}

	public byte[] getRawSecretKey() {
		return rawSecretKey;
	}

	public String getFileName() {
		return fileName;
	}
	
}

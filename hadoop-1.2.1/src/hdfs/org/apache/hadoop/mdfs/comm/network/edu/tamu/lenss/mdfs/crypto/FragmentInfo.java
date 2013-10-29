package edu.tamu.lenss.mdfs.crypto;

import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import adhoc.etc.IOUtilities;

import com.tiemens.secretshare.engine.SecretShare;
import com.tiemens.secretshare.engine.SecretShare.PublicInfo;
import com.tiemens.secretshare.engine.SecretShare.ShareInfo;

public class FragmentInfo implements Serializable {
	private static final long serialVersionUID = 1L;
	//private static final String TAG = FragmentInfo.class.getSimpleName();
	public static final int DATA_TYPE = 0;
	public static final int CODING_TYPE = 1;
	
	private String _filename;
	private int _type;
	private long _filesize;
	private byte[] _fragment;
	private long _lastModifiedTS;
	private int _fragmentNumber;
	private int _kNumber;
	private int _nNumber;
	
	public FragmentInfo(String filename, int fragmentType, long filesize, 
			byte[] fragment, int fragmentNumber, int kNumber, int nNumber, long lastModified) {
		super();
		_filename = filename;
		_fragment = fragment;
		_fragmentNumber = fragmentNumber;
		_kNumber = kNumber;
		_nNumber = nNumber;
		_lastModifiedTS = lastModified;
		_type = fragmentType;
		_filesize = filesize;
	}
	
	
	/**
	 * @param filename - the filename
	 * @param timestamp - the timestamp
	 * @param fragmentNum - the fragment number
	 * @return the hash value of the fragment given the filename, timestamp, and fragment number
	 * @throws NoSuchAlgorithmException
	 */
	public static String generateFragmentHash(String filename, long timestamp, int fragmentNum) 
	throws NoSuchAlgorithmException {
		return getHash(filename + timestamp + fragmentNum);
	}
	
	
	/**
	 * @param _hashString input string to perform hash on
	 * @return MD5 has based on the input string
	 * @throws NoSuchAlgorithmException
	 * 
	 * Private function that actually performs the MD5 hash for getFileHash() and
	 * getFragmentHash()
	 */
	private static String getHash(String _hashString) throws NoSuchAlgorithmException {
		MessageDigest md = null;
		md = MessageDigest.getInstance("MD5");
		md.update(_hashString.getBytes());
		byte byteData[] = md.digest();
		StringBuffer sb = new StringBuffer();
        for (int i = 0; i < byteData.length; i++) {
        	sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
        }
        return sb.toString();	
	}
	
	/**
	 * @return the k threshold value for the fragment
	 */
	public int getK() {
		return _kNumber;
	}
	
	/**
	 * @return the n value indicating total number of fragments generated
	 */
	public int getN() {
		return _nNumber;
	}
	
	/**
	 * @return the index value between 0 and n-1
	 */
	public int getFragmentNumber() {
		return _fragmentNumber;
	}
	
	/**
	 * @return the filename of the file
	 */
	public String getFileName() {
		return _filename;
	}
	
	/**
	 * @return the byte[] representation of this fragment
	 */
	public byte[] getFragment() {
		return _fragment;
	}
	
	/**
	 * @return class content of DATA_TYPE or CODING_TYPE
	 */
	public int getType() {
		return _type;
	}
	
	/**
	 * @return the file size of the encoded file
	 */
	public long getFilesize() {
		return _filesize;
	}
	
	/**
	 * @return the last modified timestamp of the file
	 */
	public long getTimestamp() {
		return _lastModifiedTS;
	}
	
	public byte[] toByteArray(){
		return IOUtilities.objectToByteArray(this);
	}
	
	public static FragmentInfo parseFromBytes(byte[] bytes){
		return IOUtilities.bytesToObject(bytes, FragmentInfo.class);
	}
	
	
	/* Serializable key fragment information*/
	public static class KeyShareInfo implements Serializable{
		private static final long serialVersionUID = 1L;
		private int index, k, n;
		private BigInteger sharedFrags;
		private String fileName;
		//BigInteger modulus = SecretShare.getPrimeUsedFor384bitSecretPayload();
		
		public KeyShareInfo(BigInteger fragment, int index, int kVal, int nVal, String fName){
			this.sharedFrags = fragment;
			this.index = index;
			this.k = kVal;
			this.n = nVal;
			this.fileName = fName;
		}
		
		public KeyShareInfo(ShareInfo info, int kVal, int nVal, String fName){
			this(info.getShare(), info.getX(), kVal, nVal, fName );
		}
		
		public ShareInfo getShareInfo(){
			PublicInfo info = new PublicInfo(n, k, SecretShare.getPrimeUsedFor384bitSecretPayload(), null);
			return new ShareInfo(index, sharedFrags, info);
		}

		public int getIndex() {
			return index;
		}

		public int getK() {
			return k;
		}

		public int getN() {
			return n;
		}

		public BigInteger getSharedFrags() {
			return sharedFrags;
		}

		public String getFileName() {
			return fileName;
		}
	}
}

package edu.tamu.lenss.mdfs.models;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

/**
 * This class is used to store the information of any created file
 * @author Jay
 */
public class MDFSFileInfo implements Serializable {
	private static final long serialVersionUID = 1L;
	private final long createdTime;
	private final String fileName;
	private final boolean fragmented;
	private long  lastModifiedTime;
	private long fileLength;
	private int k1, n1, k2, n2;
	private int creator;
	
	private Set<Integer> keyStorage;
	private Set<Integer> fileStorage;
	
	public MDFSFileInfo(String fileName, long time, boolean isFragmented){
		this.fileName = fileName;
		this.createdTime = time;
		this.fragmented = isFragmented;
	}

	public long getLastModifiedTime() {
		return lastModifiedTime;
	}

	public void setLastModifiedTime(long lastModifiedTime) {
		this.lastModifiedTime = lastModifiedTime;
	}

	public int getCreator() {
		return creator;
	}

	public void setCreator(int creator) {
		this.creator = creator;
	}

	public Set<Integer> getKeyStorage() {
		return keyStorage;
	}

	public void setKeyStorage(Set<Integer> keyStorage) {
		this.keyStorage = keyStorage;
	}

	public Set<Integer> getFileStorage() {
		return fileStorage;
	}

	public void setFileStorage(Set<Integer> fileStorage) {
		this.fileStorage = fileStorage;
	}

	public long getCreatedTime() {
		return createdTime;
	}

	public String getFileName() {
		return fileName;
	}

	public int getK1() {
		return k1;
	}

	public int getN1() {
		return n1;
	}

	public int getK2() {
		return k2;
	}

	public int getN2() {
		return n2;
	}
	
	public void setFragmentsParms(int n1, int k1, int n2, int k2){
		this.n1 = n1;
		this.n2 = n2;
		this.k1 = k1;
		this.k2 = k2;
	}

	public boolean isFragmented() {
		return fragmented;
	}

	public long getFileLength() {
		return fileLength;
	}

	public void setFileLength(long fileLength) {
		this.fileLength = fileLength;
	}
	
	/**
	 * Return a directory of file fragments
	 * @param fileName
	 * @param createdTime
	 * @return fileName_MMddyyy_HHmmss
	 */
	public static String getDirName(String fileName, long createdTime){
		SimpleDateFormat format =
	            new SimpleDateFormat("MMddyyyy_HHmmss");
		return fileName + "__" + format.format(new Date(createdTime));
	}
}

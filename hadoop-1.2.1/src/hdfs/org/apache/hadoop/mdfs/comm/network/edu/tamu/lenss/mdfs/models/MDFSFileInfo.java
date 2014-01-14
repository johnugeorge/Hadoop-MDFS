package edu.tamu.lenss.mdfs.models;

import java.io.Serializable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;
import java.util.HashSet;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * This class is used to store the information of any created file
 * @author Jay
 */
public class MDFSFileInfo implements Serializable,Writable {
	private static final long serialVersionUID = 1L;
	private long createdTime;
	private String fileName;
	private boolean fragmented;
	private long  lastModifiedTime;
	private long fileLength;
	private int k1, n1, k2, n2;
	private int creator;
	
	private Set<Integer> keyStorage;
	private Set<Integer> fileStorage;
	

	static {                                      // register a ctor
		WritableFactories.setFactory
			(MDFSFileInfo.class,
			 new WritableFactory() {
				 public Writable newInstance() { return new MDFSFileInfo(); }
			 });
	}

	public MDFSFileInfo(){
		fileName=null;
		createdTime = 0;
	}

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
		//SimpleDateFormat format =
	        //    new SimpleDateFormat("MMddyyyy_HHmmss");
		String tmp= fileName.substring(fileName.lastIndexOf("/")+1);
		return tmp + "__" + createdTime;
	}

		
	/**
	 * This is the file name without the path.
	 * @param fileName
	 * @return
	 */
	public static String getShortFileName(String fileName){
		String tmp= fileName.substring(fileName.lastIndexOf("/")+1);
		return tmp;
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(createdTime);
		out.writeUTF(fileName);
		out.writeBoolean(fragmented);
		out.writeLong(lastModifiedTime);
		out.writeLong(fileLength);
		out.writeInt(k1);
		out.writeInt(n1);
		out.writeInt(k2);
		out.writeInt(n2);
		out.writeInt(creator);
		int keySize=keyStorage.size();
		out.writeInt(keySize);
		if(keySize > 0){
			for(Integer i: keyStorage){
				out.writeInt(i);
			}
		}
		int fileSize=fileStorage.size();
		out.writeInt(fileSize);
		if(fileSize > 0){
			for(Integer i: fileStorage){
				out.writeInt(i);
			}
		}
	}

	public void readFields(DataInput in) throws IOException {
		createdTime = in.readLong();
		fileName = in.readUTF();
		fragmented = in.readBoolean();
		lastModifiedTime = in.readLong();
		fileLength = in.readLong();
		k1= in.readInt();
		n1= in.readInt();
		k2= in.readInt();
		n2= in.readInt();
		creator= in.readInt();

		int keySize=in.readInt();
		keyStorage= new HashSet<Integer>();
		for(int i=0;i< keySize;i++){
			keyStorage.add(in.readInt());
		}
		int fileSize=in.readInt();
		fileStorage= new HashSet<Integer>();
		for(int i=0;i< fileSize;i++){
			fileStorage.add(in.readInt());
		}
	}

}

package edu.tamu.lenss.mdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import adhoc.etc.IOUtilities;
import edu.tamu.lenss.mdfs.models.MDFSFileInfo;
import edu.tamu.lenss.mdfs.utils.AndroidIOUtils;

/**
 * This class track the current status of MDFS File System. <br>
 * Available files in the network, local available files, or local available fragments...
 * @author Jay
 *
 */
public class MDFSDirectory implements Serializable {

	private static final long serialVersionUID = 1L;
	private Map<Long, MDFSFileInfo> fileMap;	// Use File Creation Time as the key now. Should use UUID
	private Map<String, Long> nameToKeyMap;		// Used to map from file name to file ID(createdTime)
	
	private Map<Long, Integer> keyFragMap;		// Used to map from fileId to key fragment#
	private Map<Long, Set<Integer>> fileFragMap; // Used to map from fileId to file fragment#
	private Set<Long> encryptedFileSet;			// A set of files that have been combined, but encrypted.
	private Set<Long> decryptedFileSet;			// A set of files that have been decrypted temporarily
	
	public MDFSDirectory(){
		fileMap = new HashMap<Long, MDFSFileInfo>();
		nameToKeyMap = new HashMap<String, Long>();
		keyFragMap = new HashMap<Long, Integer>();
		fileFragMap = new HashMap<Long, Set<Integer>>();
		encryptedFileSet = new HashSet<Long>();
		decryptedFileSet = new HashSet<Long>();
	}
	
	/**
	 * Return null if the fileId does not exist in the directory
	 * @param fileId
	 * @return
	 */
	public MDFSFileInfo getFileInfo(long fileId){
		return fileMap.get(fileId);
	}
	public MDFSFileInfo getFileInfo(String fName){
		Long fileId = nameToKeyMap.get(fName);
		if(fileId != null)
			return fileMap.get(fileId);
		else
			return null;
	}
	
	
	/**
	 * @return	A List of all available files. The List may be empty
	 */
	public List<MDFSFileInfo> getFileList(){
		List<MDFSFileInfo> list;
		if(!fileMap.isEmpty())
			 list = new ArrayList<MDFSFileInfo>(fileMap.values());
		else
			list = new ArrayList<MDFSFileInfo>();;
		return list;
	}
	
	/**
	 * @param name
	 * @return	-1 if the the id is not available
	 */
	public long getFileIdByName(String name){
		Long id = nameToKeyMap.get(name);
		if(id != null)
			return id;
		else
			return -1;
	}
	
	/**
	 * @param fileId
	 * @return -1 if there is no key fragment available
	 */
	public int getStoredKeyIndex(long fileId){
		Integer idx = keyFragMap.get(fileId);
		if(idx != null)
			return idx;
		else
			return -1;
	}
	public int getStoredKeyIndex(String fName){
		Long fileId = nameToKeyMap.get(fName);
		if(fileId != null)
			return getStoredKeyIndex(fileId);
		else
			return -1;
	}
	
	/**
	 * @param fileId
	 * @return	null if there is no file fragment avaiable
	 */
	public Set<Integer> getStoredFileIndex(long fileId){
		return fileFragMap.get(fileId);
	}
	public Set<Integer> getStoredFileIndex(String fName){
		Long fileId = nameToKeyMap.get(fName);
		if(fileId != null)
			return fileFragMap.get(fileId);
		else
			return null;
	}
	
	
	public void addFile(MDFSFileInfo file){
		fileMap.put(file.getCreatedTime(), file);
		nameToKeyMap.put(file.getFileName(), file.getCreatedTime());
	}
	
	/**
	 * FileId is just the file created time now..
	 * @param fileId
	 */
	public void removeFile(long fileId){
		if(fileMap.containsKey(fileId)){
			String name = fileMap.get(fileId).getFileName();
			nameToKeyMap.remove(name);
		}
		fileMap.remove(fileId);
	}
	
	public void addKeyFragment(long fileId, int keyIndex){
		keyFragMap.put(fileId, keyIndex);
	}
	public void addKeyFragment(String fileName, int keyIndex){
		Long fileId = nameToKeyMap.get(fileName);
		if(fileId != null )
			keyFragMap.put(fileId, keyIndex);
	}
	
	public void removeKeyFragment(long fileId){
		keyFragMap.remove(fileId);
	}
	public void removeKeyFragment(String fileName){
		Long fileId = nameToKeyMap.get(fileName);
		if(fileId != null )
			keyFragMap.remove(fileId);
	}
	
	public void addFileFragment(long fileId, int fileIndex){
		if(fileFragMap.containsKey(fileId)){
			fileFragMap.get(fileId).add(fileIndex);
		}
		else{
			HashSet<Integer> fFrag = new HashSet<Integer>();
			fFrag.add(fileIndex);
			fileFragMap.put(fileId, fFrag);
		}
	}
	
	/**
	 * Not recommend. The fileName may not be available in the directory when the fragment is downloaded. <br>
	 * FileName map is added after receiving the directory update
	 * @param fileName
	 * @param fileIndex
	 */
	public void addFileFragment(String fileName, int fileIndex){
		Long fileId = nameToKeyMap.get(fileName);
		if(fileId == null )
			return;
		addFileFragment(fileId, fileIndex);
	}
	
	public void addFileFragment(long fileId, Set<Integer> fileIndex){
		if(fileFragMap.containsKey(fileId)){
			fileFragMap.get(fileId).addAll(fileIndex);
		}
		else{
			HashSet<Integer> fFrag = new HashSet<Integer>();
			fFrag.addAll(fileIndex);
			fileFragMap.put(fileId, fFrag);
		}
	}
	public void addFileFragment(String fileName, Set<Integer> fileIndex){
		Long fileId = nameToKeyMap.get(fileName);
		if(fileId == null )
			return;
		addFileFragment(fileId, fileIndex);
	}
	
	public void removeFileFragment(long fileId){
		fileFragMap.remove(fileId);
	}
	
	/**
	 * Remove ALL fragments of this file
	 * @param fileName
	 */
	public void removeFileFragment(String fileName){
		Long fileId = nameToKeyMap.get(fileName);
		if(fileId != null )
			fileFragMap.remove(fileId);
	}
	
	public void addEncryptedFile(long fileId){
		encryptedFileSet.add(fileId);
	}
	
	/**
	 * Not recommend. The fileName may not be available in the directory when the fragment is downloaded. <br>
	 * FileName map is added after receiving the directory update
	 * @param fileName
	 */
	public void addEncryptedFile(String fileName){
		Long fileId = nameToKeyMap.get(fileName);
		if(fileId != null )
			encryptedFileSet.add(fileId);
	}
	
	public void removeEncryptedFile(long fileId){
		encryptedFileSet.remove(fileId);
	}
	
	public void removeEncryptedFile(String fileName){
		Long fileId = nameToKeyMap.get(fileName);
		if(fileId != null )
			encryptedFileSet.remove(fileId);
	}
	
	public void addDecryptedFile(long fileId){
		decryptedFileSet.add(fileId);
	}
	
	/**
	 * Not recommend. The fileName may not be available in the directory when the fragment is downloaded. <br>
	 * FileName map is added after receiving the directory update
	 * @param fileName
	 */
	public void addDecryptedFile(String fileName){
		Long fileId = nameToKeyMap.get(fileName);
		if(fileId != null )
			decryptedFileSet.add(fileId);
	}
	
	public void removeDecryptedFile(long fileId){
		decryptedFileSet.remove(fileId);
	}
	public void removeDecryptedFile(String fileName){
		Long fileId = nameToKeyMap.get(fileName);
		if(fileId != null )
			decryptedFileSet.remove(fileId);
	}
	
	public boolean isEncryptedFileCached(long fileId){
		return encryptedFileSet.contains(fileId);
	}
	
	public boolean isDecryptedFileCached(long fileId){
		return decryptedFileSet.contains(fileId);
	}
	
	/**
	 * Clear the entire directory information
	 */
	public void clearAll() {
		// Reset the maps
		fileMap.clear();
		keyFragMap.clear();
		nameToKeyMap.clear();
		fileFragMap.clear();
		encryptedFileSet.clear();
		decryptedFileSet.clear();
		encryptedFileSet.clear();
		decryptedFileSet.clear();
	}
	
	/**
	 * Make sure the directory is synchronized with the physical files
	 */
	public void syncLocal(){
		File rootDir = AndroidIOUtils.getExternalFile(Constants.DIR_ROOT);
		if(!rootDir.exists())
			return;			// Don't need to sync at all
		File[] directories = rootDir.listFiles();
		String fileName;
		
		// Reset the maps
		keyFragMap.clear();	 
		fileFragMap.clear();
		encryptedFileSet.clear();
		decryptedFileSet.clear();
		
		String dirName;
		for(File fileDir : directories){
			if(!fileDir.isDirectory())
				continue;	// We only check for directories
			dirName = fileDir.getName();
			if(dirName.equalsIgnoreCase("decrypted")){
				File[] decFiles = fileDir.listFiles();
				for(File f : decFiles){
					addDecryptedFile(f.getName().trim());
				}
			}
			else if(dirName.equalsIgnoreCase("encrypted")){
				File[] encFiles = fileDir.listFiles();
				for(File f : encFiles){
					addEncryptedFile(f.getName().trim());
				}
			}
			else if(dirName.contains("__")){
				fileName = dirName.substring(0, dirName.indexOf("__"));
				fileName = fileName.trim();
				File[] fileFrags = fileDir.listFiles();
				int index;
				String fragName;
				for(File frag : fileFrags){
					fragName = frag.getName();
					try{
						index = Integer.parseInt(fragName.substring(fragName.lastIndexOf("_")+1));
						if(fragName.contains("__key__")){
							addKeyFragment(fileName, index);
						}
						else if (fragName.contains("__frag__")){
							addFileFragment(fileName, index);
						}
					}
					catch(NullPointerException e){
						e.printStackTrace();
					}
					catch(IndexOutOfBoundsException e){
						e.printStackTrace();
					}
					catch(NumberFormatException e){
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	/**
	 * Save the current MDFSDirectory Object to the SD Card
	 * @return true if the file is saved successfully
	 */
	public boolean saveDirectory(){
		File tmp0 = AndroidIOUtils.getExternalFile(Constants.DIR_ROOT);
		File tmp = IOUtilities.createNewFile(tmp0, Constants.NAME_MDFS_DIRECTORY);
		if(tmp == null){
			return false;
		}
		try {
			FileOutputStream fos = new FileOutputStream(tmp);
			ObjectOutputStream output = new ObjectOutputStream(fos);
			output.writeObject(this);
			output.close();
			fos.close();
			return true;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * Read the stored MDFSDirectory and return as an object
	 * @return Return a new Object if there is no history exist
	 */
	public static MDFSDirectory readDirectory(){
		File tmp = AndroidIOUtils.getExternalFile(Constants.DIR_ROOT + "/" + Constants.NAME_MDFS_DIRECTORY);
		//File tmp = IOUtilities.createNewFile(tmp0, Constants.NAME_MDFS_DIRECTORY);
		
		if(!tmp.exists())	// In case that the file does not exist
			return new MDFSDirectory();
		MDFSDirectory obj;
		try {
			FileInputStream fis = new FileInputStream(tmp);
			ObjectInputStream input = new ObjectInputStream(fis);
			obj = (MDFSDirectory)input.readObject();
			input.close();
			fis.close();
			return obj;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (StreamCorruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return new MDFSDirectory();
	}
}

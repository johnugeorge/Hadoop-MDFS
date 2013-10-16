package org.apache.hadoop.mdfs;


import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.mdfs.DFSUtil;
import org.apache.hadoop.mdfs.protocol.Block;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * We keep an in-memory representation of the file/block hierarchy.
 * This is a base MDFSINode class containing common fields for file and 
 * directory inodes.
 */
abstract class MDFSINode implements Comparable<byte[]>{
  protected byte[] fileName;
  protected MDFSINodeDirectory parent;
  protected long lastModifiedTime;
  protected long accessTime;
  protected final long createdTime;
  protected static final long serialVersionUID = 1L;
  protected int creator;
  protected boolean isFragmented;
  protected boolean isEncrypted;
  /** Simple wrapper for two counters : 
   *  nsCount (namespace consumed) and dsCount (diskspace consumed).
   */
  static class DirCounts {
    long nsCount = 0;
    long dsCount = 0;
    
    /** returns namespace count */
    long getNsCount() {
      return nsCount;
    }
    /** returns diskspace count */
    long getDsCount() {
      return dsCount;
    }
  }
  
  //Only updated by updatePermissionStatus(...).
  //Other codes should not modify it.
  private long permission;

  private static enum PermissionStatusFormat {
    MODE(0, 16),
    GROUP(MODE.OFFSET + MODE.LENGTH, 25),
    USER(GROUP.OFFSET + GROUP.LENGTH, 23);

    final int OFFSET;
    final int LENGTH; //bit length
    final long MASK;

    PermissionStatusFormat(int offset, int length) {
      OFFSET = offset;
      LENGTH = length;
      MASK = ((-1L) >>> (64 - LENGTH)) << OFFSET;
    }

    long retrieve(long record) {
      return (record & MASK) >>> OFFSET;
    }

    long combine(long bits, long record) {
      return (record & ~MASK) | (bits << OFFSET);
    }
  }

  protected MDFSINode() {
    fileName = null;
    parent = null;
    lastModifiedTime = 0;
    accessTime = 0;
  }

  MDFSINode(PermissionStatus permissions, long mTime, long atime) {
    this.fileName = null;
    this.parent = null;
    this.lastModifiedTime = mTime;
    setAccessTime(atime);
    setPermissionStatus(permissions);
  }

  //compatibility with earlier code
  protected MDFSINode(String fileName, long time, boolean isFragmented){
	  this(new PermissionStatus(UserGroupInformation.getCurrentUser().getShortUserName(),
				              null, FsPermission.getDefault()), 0L, 0L);
	  setLocalName(fileName);
	  this.createdTime = time;
	  this.isFragmented = isFragmented;
	  this.isEncrypted = isFragmented;//TODO Assume that if file is fragmented, it is encrypted 
  }

  protected MDFSINode(String name, PermissionStatus permissions) {
    this(permissions, 0L, 0L);
    setLocalName(name);
  }
 

  public MDFSINode(String name, PermissionStatus permissions,long createdTime, boolean isFragmented, boolean isEncrypted){
	  this(permissions, 0L, 0L);
          setLocalName(name); 
	  this.createdTime = createdTime;
	  this.isFragmented = isFragmented;
	  this.isEncrypted = isEncrypted;

  }

  /** copy constructor
   * 
   * @param other Other node to be copied
   */
  MDFSINode(MDFSINode other) {
    setLocalName(other.getLocalName());
    this.parent = other.getParent();
    setPermissionStatus(other.getPermissionStatus());
    setLastModifiedTime(other.getLastModifiedTime());
    setAccessTime(other.getAccessTime());
  }

  /**
   * Check whether this is the root inode.
   */
  boolean isRoot() {
    return fileName.length == 0;
  }

  /** Set the {@link PermissionStatus} */
  protected void setPermissionStatus(PermissionStatus ps) {
    setUser(ps.getUserName());
    setGroup(ps.getGroupName());
    setPermission(ps.getPermission());
  }
  /** Get the {@link PermissionStatus} */
  protected PermissionStatus getPermissionStatus() {
    return new PermissionStatus(getUserName(),getGroupName(),getFsPermission());
  }

  private synchronized void updatePermissionStatus(
      PermissionStatusFormat f, long n) {
    permission = f.combine(n, permission);
  }

  /** Get user name */
  public String getUserName() {
    int n = (int)PermissionStatusFormat.USER.retrieve(permission);
    return SerialNumberManager.INSTANCE.getUser(n);
  }

  /** Set user */
  protected void setUser(String user) {
    int n = SerialNumberManager.INSTANCE.getUserSerialNumber(user);
    updatePermissionStatus(PermissionStatusFormat.USER, n);
  }
  /** Get group name */
  public String getGroupName() {
    int n = (int)PermissionStatusFormat.GROUP.retrieve(permission);
    return SerialNumberManager.INSTANCE.getGroup(n);
  }
  /** Set group */
  protected void setGroup(String group) {
    int n = SerialNumberManager.INSTANCE.getGroupSerialNumber(group);
    updatePermissionStatus(PermissionStatusFormat.GROUP, n);
  }
  /** Get the {@link FsPermission} */
  public FsPermission getFsPermission() {
    return new FsPermission(
        (short)PermissionStatusFormat.MODE.retrieve(permission));
  }
  protected short getFsPermissionShort() {
    return (short)PermissionStatusFormat.MODE.retrieve(permission);
  }
  /** Set the {@link FsPermission} of this {@link MDFSINode} */
  protected void setPermission(FsPermission permission) {
    updatePermissionStatus(PermissionStatusFormat.MODE, permission.toShort());
  }

  /**
   * Check whether it's a directory
   */
  public abstract boolean isDirectory();
  /**
   * Collect all the blocks in all children of this MDFSINode.
   * Count and return the number of files in the sub tree.
   * Also clears references since this MDFSINode is deleted.
   */
  abstract int collectSubtreeBlocksAndClear(List<Block> v);

  /**
   * @return an array of three longs. 
   * 0: length, 1: file count, 2: directory count 3: disk space
   */
  abstract long[] computeContentSummary(long[] summary);

  public final ContentSummary computeContentSummary() {
	  long[] a = computeContentSummary(new long[]{0,0,0,0});
	  return new ContentSummary(a[0], a[1], a[2], getNsQuota(), 
			  a[3], getDsQuota());
  }

  /**
   * Get the quota set for this inode
   * @return the quota if it is set; -1 otherwise
   */
  long getNsQuota() {
    return -1;
  }

  long getDsQuota() {
    return -1;
  }
  
  boolean isQuotaSet() {
    return getNsQuota() >= 0 || getDsQuota() >= 0;
  }
  
  /**
   * Adds total nubmer of names and total disk space taken under 
   * this tree to counts.
   * Returns updated counts object.
   */
  abstract DirCounts spaceConsumedInTree(DirCounts counts);
  
  /**
   * Get local file name
   * @return local file name
   */
  String getFileName() {
    return DFSUtil.bytes2String(fileName);
  }

  /**
   * Get local file name
   * @return local file name
   */
  byte[] getLocalNameBytes() {
    return fileName;
  }

  /**
   * Set local file name
   */
  void setLocalName(String name) {
    this.fileName = DFSUtil.string2Bytes(name);
  }

  byte[] getLocalName() {
    return this.fileName;
  }

  /**
   * Set local file name
   */
  void setLocalName(byte[] name) {
    this.fileName = name;
  }

  /** {@inheritDoc} */
  public String getFullPathName() {
    // Get the full path name of this inode.
    return MDFSINode.getFullPathName(this);
  }

  /** {@inheritDoc} */
  public String toString() {
    return "\"" + getLocalName() + "\":" + getPermissionStatus();
  }

  /**
   * Get parent directory 
   * @return parent MDFSINode
   */
  MDFSINodeDirectory getParent() {
    return this.parent;
  }

  /** 
   * Get last modification time of inode.
   * @return access time
   */
  public long getLastModifiedTime() {
    return this.lastModifiedTime;
  }

  /**
   * Set last modification time of inode.
   */
  void setLastModifiedTime(long modtime) {
    if (this.lastModifiedTime <= modtime) {
      this.lastModifiedTime = modtime;
    } else {
           //TODO
    }
  }

  public int getCreator() {
	  return creator;
  }

  public void setCreator(int creator) {
	  this.creator = creator;
  }

  /**
   * Always set the last modification time of inode.
   */
  void setModificationTimeForce(long modtime) {
    assert !isDirectory();
    this.lastModifiedTime = modtime;
  }

  /**
   * Get access time of inode.
   * @return access time
   */
  public long getAccessTime() {
    return accessTime;
  }

  /**
   * Set last access time of inode.
   */
  void setAccessTime(long atime) {
    accessTime = atime;
  }

  /**
   * Is this inode being constructed?
   */
  boolean isUnderConstruction() {
    return false;
  }

  

  public long getCreatedTime() {
	  return createdTime;
  }
  /**
   * Breaks file path into components.
   * @param path
   * @return array of byte arrays each of which represents 
   * a single path component.
   */
  static byte[][] getPathComponents(String path) {
    return getPathComponents(getPathNames(path));
  }

  /** Convert strings to byte arrays for path components. */
  static byte[][] getPathComponents(String[] strings) {
    if (strings.length == 0) {
      return new byte[][]{null};
    }
    byte[][] bytes = new byte[strings.length][];
    for (int i = 0; i < strings.length; i++)
      bytes[i] = DFSUtil.string2Bytes(strings[i]);
    return bytes;
  }


  static String getFullPathName(MDFSINode inode) {
	  int depth = 0;
	  for (MDFSINode i = inode; i != null; i = i.parent) {
		  depth++;
	  }
	  MDFSINode[] inodes = new MDFSINode[depth];
	  for (int i = 0; i < depth; i++) {
		  inodes[depth-i-1] = inode;
		  inode = inode.parent;
	  }
	  return getFullPathName(inodes, depth-1);
  }

  private static String getFullPathName(MDFSINode[] inodes, int pos) {
	  StringBuilder fullPathName = new StringBuilder();
	  for (int i=1; i<=pos; i++) {
		  fullPathName.append(Path.SEPARATOR_CHAR).append(inodes[i].getLocalName());
	  }
	  return fullPathName.toString();
  }

  /**
   * Breaks file path into names.
   * @param path
   * @return array of names 
   */
  static String[] getPathNames(String path) {
    if (path == null || !path.startsWith(Path.SEPARATOR)) {
      return null;
    }
    return path.split(Path.SEPARATOR);
  }

  boolean removeNode() {
    if (parent == null) {
      return false;
    } else {
      
      parent.removeChild(this);
      parent = null;
      return true;
    }
  }

  //
  // Comparable interface
  //
  public int compareTo(byte[] o) {
    return compareBytes(fileName, o);
  }

  public boolean equals(Object o) {
    if (!(o instanceof MDFSINode)) {
      return false;
    }
    return Arrays.equals(this.fileName, ((MDFSINode)o).fileName);
  }

  public int hashCode() {
    return Arrays.hashCode(this.fileName);
  }

  //
  // static methods
  //
  /**
   * Compare two byte arrays.
   * 
   * @return a negative integer, zero, or a positive integer 
   * as defined by {@link #compareTo(byte[])}.
   */
  static int compareBytes(byte[] a1, byte[] a2) {
    if (a1==a2)
        return 0;
    int len1 = (a1==null ? 0 : a1.length);
    int len2 = (a2==null ? 0 : a2.length);
    int n = Math.min(len1, len2);
    byte b1, b2;
    for (int i=0; i<n; i++) {
      b1 = a1[i];
      b2 = a2[i];
      if (b1 != b2)
        return b1 - b2;
    }
    return len1 - len2;
  }
  
}

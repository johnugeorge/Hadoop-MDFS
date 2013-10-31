package org.apache.hadoop.mdfs.protocol;


import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.mdfs.protocol.Block;

import edu.tamu.lenss.mdfs.models.NodeInfo;


class MDFSINodeFile extends MDFSINode {
  static final FsPermission UMASK = FsPermission.createImmutable((short)0111);

  //Number of bits for Block size
  static final short BLOCKBITS = 48;

  //Header mask 64-bit representation
  //Format: [16 bits for replication][48 bits for PreferredBlockSize]
  static final long HEADERMASK = 0xffffL << BLOCKBITS;

  protected long header;

  protected long fileLength;
  protected int k1, n1, k2, n2;

  protected BlockInfo blocks[] = null;

/*  MDFSINodeFile(PermissionStatus permissions,
            int nrBlocks, short replication, long modificationTime,
            long atime, long preferredBlockSize) {
    this(permissions, new BlockInfo[nrBlocks], replication,
        modificationTime, atime, preferredBlockSize);
  }
*/

  MDFSINodeFile(FsPermission permissions,
		  int nrBlocks, short replication, long preferredBlockSize) {
    super("", permissions);
    blocks = new BlockInfo[nrBlocks];
    this.setReplication(replication);
    this.setPreferredBlockSize(preferredBlockSize);

  }

  protected MDFSINodeFile() {
    super();
    blocks = null;
    header = 0;
  }
/*
  protected MDFSINodeFile(PermissionStatus permissions, BlockInfo[] blklist,
                      short replication, long modificationTime,
                      long atime, long preferredBlockSize) {
    super(permissions, modificationTime, atime);
    this.setReplication(replication);
    this.setPreferredBlockSize(preferredBlockSize);
    blocks = blklist;
  }
*/
  /**
   * Set the {@link FsPermission} of this {@link MDFSINodeFile}.
   * Since this is a file,
   * the {@link FsAction#EXECUTE} action, if any, is ignored.
   */
  protected void setPermission(FsPermission permission) {
    super.setPermission(permission.applyUMask(UMASK));
  }

  public boolean isDirectory() {
    return false;
  }

  /**
   * Get block replication for the file 
   * @return block replication value
   */
  public short getReplication() {
    return (short) ((header & HEADERMASK) >> BLOCKBITS);
  }

  public void setReplication(short replication) {
    if(replication <= 0)
       throw new IllegalArgumentException("Unexpected value for the replication");
    header = ((long)replication << BLOCKBITS) | (header & ~HEADERMASK);
  }

  /**
   * Get preferred block size for the file
   * @return preferred block size in bytes
   */
  public long getPreferredBlockSize() {
        return header & ~HEADERMASK;
  }

  public void setPreferredBlockSize(long preferredBlkSize)
  {
    if((preferredBlkSize < 0) || (preferredBlkSize > ~HEADERMASK ))
       throw new IllegalArgumentException("Unexpected value for the block size");
    header = (header & HEADERMASK) | (preferredBlkSize & ~HEADERMASK);
  }

  /**
   * Get file blocks 
   * @return file blocks
   */
  BlockInfo[] getBlocks() {
    return this.blocks;
  }

  /**
   * append array of blocks to this.blocks
   */
  void appendBlocks(MDFSINodeFile [] inodes, int totalAddedBlocks) {
    int size = this.blocks.length;
    
    BlockInfo[] newlist = new BlockInfo[size + totalAddedBlocks];
    System.arraycopy(this.blocks, 0, newlist, 0, size);
    
    for(MDFSINodeFile in: inodes) {
      System.arraycopy(in.blocks, 0, newlist, size, in.blocks.length);
      size += in.blocks.length;
    }
    
    for(BlockInfo bi: this.blocks) {
      bi.setMDFSINode(this);
    }
    this.blocks = newlist;
  }
  
  /**
   * add a block to the block list
   */
  void addBlock(BlockInfo newblock) {
    if (this.blocks == null) {
      this.blocks = new BlockInfo[1];
      this.blocks[0] = newblock;
    } else {
      int size = this.blocks.length;
      BlockInfo[] newlist = new BlockInfo[size + 1];
      System.arraycopy(this.blocks, 0, newlist, 0, size);
      newlist[size] = newblock;
      this.blocks = newlist;
    }
  }

  /**
   * Set file block
   */
  void setBlock(int idx, BlockInfo blk) {
    this.blocks[idx] = blk;
  }

  int collectSubtreeBlocksAndClear(List<BlockInfo> v) {
    parent = null;
    if (blocks != null && v != null) {
      for (BlockInfo blk : blocks) {
        v.add(blk);
        blk.setMDFSINode(null);
      }
    }
    blocks = null;
    return 1;
  }

  void printAllChildrenOfSubtrees() {

  }

  /** {@inheritDoc} */
  long[] computeContentSummary(long[] summary) {
    long bytes = 0;
    for(Block blk : blocks) {
      bytes += blk.getNumBytes();
    }
    summary[0] += bytes;
    summary[1]++;
    summary[3] += diskspaceConsumed();
    return summary;
  }

  public boolean isFragmented() {
	  return isFragmented;
  }

  public boolean isEncrypted() {
	  return isEncrypted;
  }

  @Override
  DirCounts spaceConsumedInTree(DirCounts counts) {
    counts.nsCount += 1;
    counts.dsCount += diskspaceConsumed();
    return counts;
  }

  long diskspaceConsumed() {
    return diskspaceConsumed(blocks);
  }
  
  long diskspaceConsumed(Block[] blkArr) {
    long size = 0;
    if(blkArr == null) 
      return 0;
    
    for (Block blk : blkArr) {
      if (blk != null) {
        size += blk.getNumBytes();
      }
    }
    /* If the last block is being written to, use prefferedBlockSize
     * rather than the actual block size.
     */
    if (blkArr.length > 0 && blkArr[blkArr.length-1] != null && 
        isUnderConstruction()) {
      size += getPreferredBlockSize() - blocks[blocks.length-1].getNumBytes();
    }
    return size * getReplication();
  }
  
  /**
   * Return the penultimate allocated block for this file.
   */
  Block getPenultimateBlock() {
    if (blocks == null || blocks.length <= 1) {
      return null;
    }
    return blocks[blocks.length - 2];
  }


  /**
   * Return the last block in this file, or null if there are no blocks.
   */
  Block getLastBlock() {
    if (this.blocks == null || this.blocks.length == 0)
      return null;
    return this.blocks[this.blocks.length - 1];
  }
}

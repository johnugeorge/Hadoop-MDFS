/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mdfs.protocol;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.mdfs.utils.DFSUtil;
import org.apache.hadoop.mdfs.protocol.Block;

/**
 * Directory MDFSINode class.
 */
class MDFSINodeDirectory extends MDFSINode {
  protected static final int DEFAULT_FILES_PER_DIRECTORY = 5;
  final static String ROOT_NAME = "";

  private List<MDFSINode> children;

  MDFSINodeDirectory(String name, PermissionStatus permissions) {
    super(name, permissions);
    this.children = null;
  }

 MDFSINodeDirectory(String name, FsPermission permissions) {
    super(name, permissions);
    this.children = null;
  }


 MDFSINodeDirectory(byte[] name, FsPermission permissions) {
    super(DFSUtil.bytes2String(name), permissions);
    this.children = null;
  }


  public MDFSINodeDirectory(PermissionStatus permissions, long mTime) {
    super(permissions, mTime, 0);
    this.children = null;
  }

  /** constructor */
  MDFSINodeDirectory(byte[] localName, PermissionStatus permissions, long mTime) {
    this(permissions, mTime);
    this.fileName = localName;
  }
  
  /** copy constructor
   * 
   * @param other
   */
  MDFSINodeDirectory(MDFSINodeDirectory other) {
    super(other);
    this.children = other.getChildren();
  }
  
  /**
   * Check whether it's a directory
   */
  public boolean isDirectory() {
    return true;
  }

  MDFSINode removeChild(MDFSINode node) {
    assert children != null;
    int low = Collections.binarySearch(children, node.fileName);
    if (low >= 0) {
      return children.remove(low);
    } else {
      return null;
    }
  }

  /** Replace a child that has the same name as newChild by newChild.
   * 
   * @param newChild Child node to be added
   */
  void replaceChild(MDFSINode newChild) {
    if ( children == null ) {
      throw new IllegalArgumentException("The directory is empty");
    }
    int low = Collections.binarySearch(children, newChild.fileName);
    if (low>=0) { // an old child exists so replace by the newChild
      children.set(low, newChild);
    } else {
      throw new IllegalArgumentException("No child exists to be replaced");
    }
  }
  
  MDFSINode getChild(String name) {
    return getChildMDFSINode(DFSUtil.string2Bytes(name));
  }

  private MDFSINode getChildMDFSINode(byte[] name) {
    if (children == null) {
      return null;
    }
    int low = Collections.binarySearch(children, name);
    if (low >= 0) {
      return children.get(low);
    }
    return null;
  }

  /**
   */
  private MDFSINode getNode(byte[][] components) {
    MDFSINode[] inode  = new MDFSINode[1];
    getExistingPathMDFSINodes(components, inode);
    return inode[0];
  }

  /**
   * This is the external interface
   */
  MDFSINode getNode(String path) {
    return getNode(getPathComponents(path));
  }

  /**
   * Retrieve existing MDFSINodes from a path. If existing is big enough to store
   * all path components (existing and non-existing), then existing MDFSINodes
   * will be stored starting from the root MDFSINode into existing[0]; if
   * existing is not big enough to store all path components, then only the
   * last existing and non existing MDFSINodes will be stored so that
   * existing[existing.length-1] refers to the target MDFSINode.
   * 
   * <p>
   * Example: <br>
   * Given the path /c1/c2/c3 where only /c1/c2 exists, resulting in the
   * following path components: ["","c1","c2","c3"],
   * 
   * <p>
   * <code>getExistingPathMDFSINodes(["","c1","c2"], [?])</code> should fill the
   * array with [c2] <br>
   * <code>getExistingPathMDFSINodes(["","c1","c2","c3"], [?])</code> should fill the
   * array with [null]
   * 
   * <p>
   * <code>getExistingPathMDFSINodes(["","c1","c2"], [?,?])</code> should fill the
   * array with [c1,c2] <br>
   * <code>getExistingPathMDFSINodes(["","c1","c2","c3"], [?,?])</code> should fill
   * the array with [c2,null]
   * 
   * <p>
   * <code>getExistingPathMDFSINodes(["","c1","c2"], [?,?,?,?])</code> should fill
   * the array with [rootMDFSINode,c1,c2,null], <br>
   * <code>getExistingPathMDFSINodes(["","c1","c2","c3"], [?,?,?,?])</code> should
   * fill the array with [rootMDFSINode,c1,c2,null]
   * @param components array of path component name
   * @param existing MDFSINode array to fill with existing MDFSINodes
   * @return number of existing MDFSINodes in the path
   */
  int getExistingPathMDFSINodes(byte[][] components, MDFSINode[] existing) {
    assert compareBytes(this.fileName, components[0]) == 0 :
      "Incorrect name " + getLocalName() + " expected " + components[0];

    MDFSINode curNode = this;
    int count = 0;
    int index = existing.length - components.length;
    if (index > 0)
      index = 0;
    while ((count < components.length) && (curNode != null)) {
      if (index >= 0)
        existing[index] = curNode;
      if (!curNode.isDirectory() || (count == components.length - 1))
        break; // no more child, stop here
      MDFSINodeDirectory parentDir = (MDFSINodeDirectory)curNode;
      curNode = parentDir.getChildMDFSINode(components[count + 1]);
      count += 1;
      index += 1;
    }
    return count;
  }

  /**
   * Retrieve the existing MDFSINodes along the given path. The first MDFSINode
   * always exist and is this MDFSINode.
   * 
   * @param path the path to explore
   * @return MDFSINodes array containing the existing MDFSINodes in the order they
   *         appear when following the path from the root MDFSINode to the
   *         deepest MDFSINodes. The array size will be the number of expected
   *         components in the path, and non existing components will be
   *         filled with null
   *         
   * @see #getExistingPathMDFSINodes(byte[][], MDFSINode[])
   */
  MDFSINode[] getExistingPathMDFSINodes(String path) {
    byte[][] components = getPathComponents(path);
    MDFSINode[] inodes = new MDFSINode[components.length];

    this.getExistingPathMDFSINodes(components, inodes);
    
    return inodes;
  }

  /**
   * Add a child inode to the directory.
   * 
   * @param node MDFSINode to insert
   * @param inheritPermission inherit permission from parent?
   * @return  null if the child with this name already exists; 
   *          node, otherwise
   */
  <T extends MDFSINode> T addChild(final T node, boolean inheritPermission) {
    if (inheritPermission) {
      FsPermission p = getFsPermission();
      //make sure the  permission has wx for the user
      if (!p.getUserAction().implies(FsAction.WRITE_EXECUTE)) {
        p = new FsPermission(p.getUserAction().or(FsAction.WRITE_EXECUTE),
            p.getGroupAction(), p.getOtherAction());
      }
      node.setPermission(p);
    }

    if (children == null) {
      children = new ArrayList<MDFSINode>(DEFAULT_FILES_PER_DIRECTORY);
    }

    int low = Collections.binarySearch(children, node.fileName);
    if(low >= 0)
      return null;
    node.parent = this;
    children.add(-low - 1, node);
    // update modification time of the parent directory
    setLastModifiedTime(node.getLastModifiedTime());
    if (node.getGroupName() == null) {
      node.setGroup(getGroupName());
    }
    return node;
  }

  /**
   * Given a child's name, return the index of the next child
   * 
   * @param name a child's name
   * @return the index of the next child
   */
  int nextChild(byte[] name) {
    if (name.length == 0) { // empty name
      return 0;
    }
    int nextPos = Collections.binarySearch(children, name) + 1;
    if (nextPos >= 0) {
      return nextPos;
    }
    return -nextPos;
  }
  
  /**
   * Equivalent to addNode(path, newNode, false).
   * @see #addNode(String, MDFSINode, boolean)
   */
  <T extends MDFSINode> T addNode(String path, T newNode) throws FileNotFoundException {
    return addNode(path, newNode, false);
  }
  /**
   * Add new MDFSINode to the file tree.
   * Find the parent and insert 
   * 
   * @param path file path
   * @param newNode MDFSINode to be added
   * @param inheritPermission If true, copy the parent's permission to newNode.
   * @return null if the node already exists; inserted MDFSINode, otherwise
   * @throws FileNotFoundException if parent does not exist or 
   * is not a directory.
   */
  <T extends MDFSINode> T addNode(String path, T newNode, boolean inheritPermission
      ) throws FileNotFoundException {
    if(addToParent(path, newNode, null, inheritPermission) == null)
      return null;
    return newNode;
  }

  /**
   * Add new inode to the parent if specified.
   * Optimized version of addNode() if parent is not null.
   * 
   * @return  parent MDFSINode if new inode is inserted
   *          or null if it already exists.
   * @throws  FileNotFoundException if parent does not exist or 
   *          is not a directory.
   */
  <T extends MDFSINode> MDFSINodeDirectory addToParent(
                                      String path,
                                      T newNode,
                                      MDFSINodeDirectory parent,
                                      boolean inheritPermission
                                    ) throws FileNotFoundException {
    byte[][] pathComponents = getPathComponents(path);
    assert pathComponents != null : "Incorrect path " + path;
    int pathLen = pathComponents.length;
    if (pathLen < 2)  // add root
      return null;
    if(parent == null) {
      // Gets the parent MDFSINode
      MDFSINode[] inodes  = new MDFSINode[2];
      getExistingPathMDFSINodes(pathComponents, inodes);
      MDFSINode inode = inodes[0];
      if (inode == null) {
        throw new FileNotFoundException("Parent path does not exist: "+path);
      }
      if (!inode.isDirectory()) {
        throw new FileNotFoundException("Parent path is not a directory: "+path);
      }
      parent = (MDFSINodeDirectory)inode;
    }
    // insert into the parent children list
    newNode.fileName = pathComponents[pathLen-1];
    if(parent.addChild(newNode, inheritPermission) == null)
      return null;
    return parent;
  }

  /** {@inheritDoc} */
  DirCounts spaceConsumedInTree(DirCounts counts) {
    counts.nsCount += 1;
    if (children != null) {
      for (MDFSINode child : children) {
        child.spaceConsumedInTree(counts);
      }
    }
    return counts;    
  }

  /** {@inheritDoc} */
  long[] computeContentSummary(long[] summary) {
    // Walk through the children of this node, using a new summary array
    // for the (sub)tree rooted at this node
    assert 4 == summary.length;
    long[] subtreeSummary = new long[]{0,0,0,0};
    if (children != null) {
      for (MDFSINode child : children) {
        child.computeContentSummary(subtreeSummary);
      }
    }

    // update the passed summary array with the values for this node's subtree
    for (int i = 0; i < summary.length; i++) {
      summary[i] += subtreeSummary[i];
    }

    summary[2]++;
    return summary;
  }

  /**
   */
  List<MDFSINode> getChildren() {
    return children==null ? new ArrayList<MDFSINode>() : children;
  }
  List<MDFSINode> getChildrenRaw() {
    return children;
  }

  int collectSubtreeBlocksAndClear(List<BlockInfo> v) {
    int total = 1;
    if (children == null) {
      return total;
    }
    for (MDFSINode child : children) {
      total += child.collectSubtreeBlocksAndClear(v);
    }
    parent = null;
    children = null;
    return total;
  }


  void printAllChildrenOfSubtrees() {
    if (children == null) {
      return;
    }
    for (MDFSINode child : children) {
	        child.printAllChildrenOfSubtrees();
		System.out.println(" Child of "+ child.getParent().getFileName()+"  is  "+ child.getFileName()+ " created at "+getCreatedTime());         

    }
  }
}

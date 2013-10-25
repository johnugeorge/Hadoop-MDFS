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


import org.apache.hadoop.io.*;
import org.apache.hadoop.security.token.Token;

import edu.tamu.lenss.mdfs.models.NodeInfo;
import java.io.*;

/****************************************************
 * A LocatedBlock is a pair of Block, NodeInfo[]
 * objects.  It tells where to find a Block.
 * 
 ****************************************************/
public class LocatedBlock implements Writable {

  static {                                      // register a ctor
    WritableFactories.setFactory
      (LocatedBlock.class,
       new WritableFactory() {
         public Writable newInstance() { return new LocatedBlock(); }
       });
  }

  private BlockInfo b;
  private long offset;  // offset of the first byte of the block in the file

  public LocatedBlock(BlockInfo b, long startOffset) {
    this.b = b;
    this.offset = startOffset;
  }


  public LocatedBlock() {
	  this(new BlockInfo(), 0L);
  }
  /**
   */
  public Block getBlock() {
    return b;
  }

  /**
   */
  
  public long getStartOffset() {
    return offset;
  }
  
  public long getBlockSize() {
    return b.getNumBytes();
  }

  void setStartOffset(long value) {
    this.offset = value;
  }
  ///////////////////////////////////////////
  // Writable
  ///////////////////////////////////////////
  public void write(DataOutput out) throws IOException {
	  out.writeLong(offset);
	  b.write(out);
  }

  public void readFields(DataInput in) throws IOException {
	  offset = in.readLong();
	  this.b = new BlockInfo();
	  b.readFields(in);
  }
}

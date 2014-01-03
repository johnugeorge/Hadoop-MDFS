package org.apache.hadoop.mdfs.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

import edu.tamu.lenss.mdfs.models.MDFSFileInfo;


/**
 *  * 
 *   * @author johnu
 *    */
public class MDFSInfoList implements Writable {

	static {                                      // register a ctor
		WritableFactories.setFactory
			(MDFSInfoList.class,
			 new WritableFactory() {
				 public Writable newInstance() { return new MDFSInfoList(); }
			 });
	}

	private List<MDFSFileInfo> itemSet;

	/**
	 *          * Constructor.
	 *                   */
	public MDFSInfoList() {

	}

	public MDFSInfoList(List<MDFSFileInfo> itemSet) {

		this.itemSet = itemSet;
	}

	public String toString() {

		return itemSet.toString();
	}

	public int size() {

		return itemSet.size();
	}


	public void readFields(DataInput in) throws IOException {

		if (this.itemSet != null) {
			this.itemSet.clear();
		} else {
			this.itemSet = new ArrayList<MDFSFileInfo>();
		}
		int count = in.readInt();
		while (count-- > 0) {
			MDFSFileInfo fInfo = new MDFSFileInfo();
			fInfo.readFields(in);
			itemSet.add(fInfo);
		}
	}

	public void write(DataOutput out) throws IOException {

		out.writeInt(itemSet.size());
		for (MDFSFileInfo item : itemSet) {
			item.write(out);
		}
	}


	public List<MDFSFileInfo> getItemSet() {

		return itemSet;
	}

	public void setItemSet(List<MDFSFileInfo> itemSet) {

		this.itemSet = itemSet;
	}

}



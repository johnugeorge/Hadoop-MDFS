package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
 
import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * 
 * @author amar
 */
public class SetWritable implements Writable {

	static {                                      // register a ctor
		WritableFactories.setFactory
			(SetWritable.class,
			 new WritableFactory() {
				 public Writable newInstance() { return new SetWritable(); }
			 });
	}

	private Set<Integer> itemSet;

	/**
	 * Constructor.
	 */
	public SetWritable() {

	}

	/**
	 * Constructor.
	 * 
	 * @param itemSet
	 */
	public SetWritable(Set<Integer> itemSet) {

		this.itemSet = itemSet;
	}

	public String toString() {

		return itemSet.toString();
	}

	public int size() {

		return itemSet.size();
	}

	public void readFields(DataInput in) throws IOException {

		// First clear the set. Otherwise we will just accumulate
		// entries every time this method is called.
		if (this.itemSet != null) {
			this.itemSet.clear();
		} else {
			this.itemSet = new HashSet<Integer>();
		}
		int count = in.readInt();
		while (count-- > 0) {
			itemSet.add(in.readInt());
		}
	}

	public void write(DataOutput out) throws IOException {

		out.writeInt(itemSet.size());
		for (int item : itemSet) {
			out.writeInt(item);
		}
	}



	/**
	 * Gets the itemSet.
	 * 
	 * @return itemSet.
	 */
	public Set<Integer> getItemSet() {

		return itemSet;
	}

	public void setItemSet(Set<Integer> itemSet) {

		this.itemSet = itemSet;
	}
}

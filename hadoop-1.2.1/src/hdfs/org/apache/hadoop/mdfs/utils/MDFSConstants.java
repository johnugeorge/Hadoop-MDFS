package org.apache.hadoop.mdfs;


enum MountFlags {
	O_WRONLY (1<<1), O_CREAT (1<<2),O_RDONLY (1<<3) ,O_TRUNCAT (1<<4),O_APPEND(1<<5);


	private int value;

	MountFlags(int numVal) {
		this.value = numVal;
	}

	public int getValue() {
		return value;
	}
}

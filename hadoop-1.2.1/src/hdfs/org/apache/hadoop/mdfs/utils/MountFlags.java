package org.apache.hadoop.mdfs.utils;


public enum MountFlags {
	O_WRONLY (1), O_CREAT (2),O_RDONLY (3) ,O_TRUNCAT (4),O_APPEND(5);

	private int enumVal;
	private int value;

	MountFlags(int numVal) {
		this.enumVal=numVal;
		this.value = 1<<numVal;
	}

	public int getValue() {
		return value;
	}

	public boolean isSet(int value){
		return ((value & (1<<enumVal)) != 0)?true:false;
	}
}

package org.apache.hadoop.mdfs.test;

import org.apache.hadoop.mdfs.MobileDistributedFileSystem;
import org.apache.hadoop.mdfs.protocol.MDFSDirectory;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import java.io.*;
import org.apache.hadoop.io.DataInputBuffer;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;

class TestListing{
	public static void main(String[] args)  throws Exception
	{

		MobileDistributedFileSystem fs = new MobileDistributedFileSystem();
		Configuration conf = new Configuration();
		conf.set("fs.default.name","mdfs://192.168.1.10:9000");
		fs.initialize(URI.create("mdfs://localhost:9000"),conf);


		System.out.println(" Listing Tree");
		fs.listStatus(new Path("/"));
		System.out.println(" ");
		fs.listStatus(new Path("/dir1"));
		System.out.println(" ");
		fs.listStatus(new Path("/user"));
		System.out.println(" ");
		fs.listStatus(new Path("/user/hduser"));
		System.out.println(" ");
		fs.listStatus(new Path("/user/hduser/terasort-input"));
		System.out.println(" ");


	}

}

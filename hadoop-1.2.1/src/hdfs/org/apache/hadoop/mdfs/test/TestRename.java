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

class TestRename{
	public static void main(String[] args)  throws Exception
	{

		MobileDistributedFileSystem fs = new MobileDistributedFileSystem();
		Configuration conf = new Configuration();
		conf.set("fs.default.name","mdfs://192.168.1.10:9000");
		fs.initialize(URI.create("mdfs://localhost:9000"),conf);

		Path path=new Path("/dir3/");
		fs.mkdirs(path,FsPermission.getDefault());
		
		
		String line=" First String ";
		path=new Path("/dir3/FileTest.txt");
		System.out.println(" going to Rename ");

		fs.rename(new Path("/dir3/FileTest.txt"),new Path("/dir3/FileTest2.txt"));
		fs.listStatus(new Path("/"));
		System.out.println(" ");
		fs.listStatus(new Path("/dir3"));
		System.out.println(" ");



		fs.rename(new Path("/dir3/FileTest2.txt"),new Path("/dir1/FileTest3.txt"));
		fs.listStatus(new Path("/"));
		System.out.println(" ");
		fs.listStatus(new Path("/dir3"));
		System.out.println(" ");
		fs.listStatus(new Path("/dir1"));
		System.out.println(" ");


	}

}

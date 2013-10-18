package org.apache.hadoop.mdfs.test;

import org.apache.hadoop.mdfs.MobileDistributedFileSystem;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;



class TestMDFS{
	public static void main(String[] args)  throws Exception
	{

		MobileDistributedFileSystem fs = new MobileDistributedFileSystem();
		fs.initialize(URI.create("mdfs://localhost:9000"),null);

		Path path=new Path("/data");
		fs.mkdirs(path,FsPermission.getDefault());
	}
}

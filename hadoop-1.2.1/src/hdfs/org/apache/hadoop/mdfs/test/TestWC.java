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

class TestWC{
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
		System.out.println(" going to write");
		OutputStream out=fs.create(path,FsPermission.getDefault(),false,(int)4192,(short)1,(long)4192,null);
		//OutputStream appendout=fs.append(path,4192,null);//append
		BufferedWriter br = new BufferedWriter( new OutputStreamWriter( out, "UTF-8" ) );
		int i=0;
		//while(i<10000){
		br.write("Hello World");
		br.write(" done iHello Hello World world Hello");
		//i++;}
		br.close();
		fs.listStatus(new Path("/"));
		System.out.println(" ");
		fs.listStatus(new Path("/dir3"));
		System.out.println(" ");


	}

}

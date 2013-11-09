package org.apache.hadoop.mdfs.test;

import org.apache.hadoop.mdfs.MobileDistributedFileSystem;
import org.apache.hadoop.mdfs.protocol.MDFSDirectory;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import java.io.*;
import org.apache.hadoop.io.DataInputBuffer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;


import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;



class TestReadFile{
	public static void main(String[] args)  throws Exception
	{
		MobileDistributedFileSystem fs = new MobileDistributedFileSystem();
		Configuration conf = new Configuration();
		conf.set("fs.default.name","mdfs://192.168.1.10:9000");
		fs.initialize(URI.create("mdfs://localhost:9000"),conf);
		while(true){
			try{

				System.out.println(" Reading complete file  ");
				Console console = System.console();
				if (console == null) {
					System.out.println("Unable to fetch console");
					return;
				}
				String line = console.readLine();
				FSDataInputStream in=fs.open(new Path(line));
				BufferedReader rd = new BufferedReader( new InputStreamReader( in) );

				byte[] b1 = new byte[20000];
				int a=0;
				int count=0;
				while((a=in.read()) !=-1){
					count++;
					//int bytesRead = in.read(b1, 0, 10);
					//System.out.println(" Read String is "+(char)a);
				}
				System.out.println(" Reading complete file   done");



			}finally{
			}
		}




	}

}

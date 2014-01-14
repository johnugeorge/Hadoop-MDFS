package org.apache.hadoop.mdfs.util;

import org.apache.hadoop.mdfs.MobileDistributedFileSystem;
import org.apache.hadoop.mdfs.protocol.MDFSDirectory;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.io.DataInputBuffer;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;

class MDFSShell{
	public static void main(String[] args)  throws Exception
	{

		MobileDistributedFileSystem fs = new MobileDistributedFileSystem();
		Configuration conf = new Configuration();
		conf.set("fs.default.name","mdfs://192.168.1.10:9000");
		fs.initialize(URI.create("mdfs://localhost:9000"),conf);

		String input="";
		Scanner reader = new Scanner(System.in);
		System.out.println("Enter command");
		input = reader.nextLine();

		while(!input.contains("exit")){
			args = input.split(" ");

			if(args[0].contains("-createFromLocalFile") ){
				File f = new File(args[1]);
				if(f  != null && f.exists()){
					System.out.println("Start to create a file from Local path "+args[1]+" into remote path "+args[2]);
					InputStream is = null;
					OutputStream os = null;
					try {
						is = new FileInputStream(args[1]);
						os = fs.create(new Path(args[2]),FsPermission.getDefault(),false,(int)4192,(short)1,(long)4192*1024,null);
						byte[] buffer = new byte[1024];
						int length;
						while ((length = is.read(buffer)) > 0) {
							os.write(buffer, 0, length);
						}
					} finally {
						is.close();
						os.close();
					}

				}
				else{
					System.out.println("File does not exist."+ args[1]);
				}
			}

			else if(args[0].contains("-createRandomFile") ){
				int size = Integer.parseInt(args[2]);
				System.out.println(" Creating File "+args[1]+" with random Contents with size "+size+" MB");
				FSDataOutputStream out=fs.create(new Path(args[1]),FsPermission.getDefault(),false,(int)4192,(short)1,(long)4*1024*1024,null);
				BufferedWriter br = new BufferedWriter( new OutputStreamWriter( out, "UTF-8" ) );
				int i=0;
				while(i<(40000*size)){
					br.write("Hello World");
					br.write("done come here");
					i++;}
				br.close();
				out.close();

			}

			else if(args[0].contains("-createMultipleRandomFiles") ){
				int noFiles = Integer.parseInt(args[2]);
				for(int j=0;j<noFiles;j++){
					System.out.println(" Creating File "+args[1]+j+" with random Contents");
					FSDataOutputStream out=fs.create(new Path(args[1]+j),FsPermission.getDefault(),false,(int)4192,(short)1,(long)4192*1024,null);
					BufferedWriter br = new BufferedWriter( new OutputStreamWriter( out, "UTF-8" ) );
					int i=0;
					while(i<10000){
						br.write("Hello World");
						br.write(" done ");
						i++;}
					br.close();
					out.close();
				}

			}

			else if(args[0].contains("-rename") ){
				System.out.println(" Renaming src "+args[1]+" to dest "+args[2]);
				fs.rename(new Path(args[1]),new Path(args[2]));
			}
			else if(args[0].contains("-delete") ){
				System.out.println(" Deleting "+args[1]);
				fs.delete(new Path(args[1]),true);

			}
			else if(args[0].contains("-mkdir") ){
				System.out.println(" Creating directory "+args[1]);
				fs.mkdirs(new Path(args[1]),FsPermission.getDefault());

			}
			else if(args[0].contains("-list") ){
				System.out.println(" Listing  "+args[1]);
				fs.listStatus(new Path(args[1]));
			}
			else if(args[0].contains("-listAllFiles") ){
				System.out.println(" Printing Complete Tree  from "+args[1]);
				fs.printTree(new Path(args[1]),true);
			}
			else if(args[0].contains("-readFile") ){
				System.out.println(" Reading File  "+args[1]);
				boolean toPrint=false;
				if(args.length >= 3)
					toPrint=Boolean.parseBoolean(args[2]);
				FSDataInputStream in=fs.open(new Path(args[1]));
				BufferedReader rd = new BufferedReader( new InputStreamReader( in) );


				StringBuilder sb = new StringBuilder();
				byte[] b1 = new byte[4096];
				String str="";
				int a=0;
				int count=0;
				System.out.println(" Read start");
				while((a=in.read(b1,0,4096)) !=-1){
					count+=a;
					//if(toPrint)
					//	sb.append((char)a);
					if(toPrint)
						str=str+new String(b1);
				}
				System.out.println(" Read end");
				if(toPrint)
					System.out.println(" Read String is "+str);
				System.out.println(" Reading complete file done. Total "+ count+ " chars");


			}
			else if(args[0].contains("-readMultipleFiles") ){
				System.out.println(" Reading File  "+args[1]);
				boolean toPrint=false;
				int noFiles=Integer.parseInt(args[2]);
				for(int j=0;j<noFiles;j++){
					FSDataInputStream in=fs.open(new Path(args[1]+j));
					BufferedReader rd = new BufferedReader( new InputStreamReader( in) );


					byte[] b1 = new byte[4096];
					int a=0;
					int count=0;
					System.out.println(" Read start");
					while((a=in.read(b1,0,4096)) !=-1){
						count+=a;
						//if(toPrint)
						//	sb.append((char)a);
					}
					System.out.println(" Read end");
					System.out.println(" Reading complete file done. Total "+ count+ " chars");
				}

			}
			else{
				System.err.println("Unrecognized arguments!");
			}
			System.out.println("Enter command (Type exit to exit):");
			input = reader.nextLine();
		}


	}
}


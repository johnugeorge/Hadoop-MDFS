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



class TestWritable{
	public static void main(String[] args)  throws Exception
	{
		MobileDistributedFileSystem fs = new MobileDistributedFileSystem();
		Configuration conf = new Configuration();
		conf.set("fs.default.name","mdfs://192.168.1.10:9000");
		fs.initialize(URI.create("mdfs://localhost:9000"),conf);
		try{

			Path path=new Path("/dir3/");
			fs.mkdirs(path,FsPermission.getDefault());


			String line=" First String ";
			path=new Path("/dir3/FileTest.txt");
			System.out.println(" going to write");
			FSDataOutputStream out=fs.create(path,FsPermission.getDefault(),false,(int)4192,(short)1,(long)4192,null);
			//OutputStream appendout=fs.append(path,4192,null);//append

			//System.out.println(" 1 ");
			byte[] split= "SPL".getBytes("UTF-8");

			out.write(split);
			long pos=out.getPos();
			System.out.println(" Present Offset "+ pos);
			out.writeInt(5);
			System.out.println(" 2 ");
			WritableUtils.writeVLong(out,100);
			WritableUtils.writeVLong(out,101);
			System.out.println(" 3 ");
			Text.writeString(out,"org.apache.hadoop.examples.terasort.TeraGen$RangeInputFormat$RangeInputSplit");
			System.out.println(" 4 ");
			WritableUtils.writeVLong(out,102);
			WritableUtils.writeVLong(out,103);
			Text.writeString(out,"third");
			System.out.println(" 5 ");
			out.writeInt(6);
			System.out.println(" 6 ");
			Text.writeString(out,"first");
			System.out.println(" 7 ");
			out.writeInt(7);


			System.out.println(" 8 ");
			Text.writeString(out,"second");
			out.close();

			System.out.println(" Reading complete file  ");
			FSDataInputStream in=fs.open(path);
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



			FSDataInputStream inFile = fs.open(path);
			System.out.println(" 1 ");
			inFile.seek(pos);
			int val1=inFile.readInt();
			System.out.println(" 2 ");
			long val2=WritableUtils.readVLong(inFile);
			long lval1=WritableUtils.readVLong(inFile);
			System.out.println(" 3 ");
			String name = Text.readString(inFile);
			System.out.println(" 4 ");
			long val3=WritableUtils.readVLong(inFile);
			long lval2=WritableUtils.readVLong(inFile);
			String test = Text.readString(inFile);
			System.out.println(" 5 ");
			int val4=inFile.readInt();
			System.out.println(" 6 ");
			String name1 = Text.readString(inFile);
			System.out.println(" 7 ");
			int val5=inFile.readInt();
			System.out.println(" 8 ");
			String name2 = Text.readString(inFile);


			System.out.println(val1+" "+val2+" "+lval1+" "+name+" "+val3+" "+lval2+" "+ test+" "+val4 +" "+name1+" "+val5+" " +name2);
			inFile.close();
		}finally{
			//fs.delete(new Path("/dir3/"),true);
		}




	}

}

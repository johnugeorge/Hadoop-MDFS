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

class TestMDFSIO{
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
		while(i<10000){
		br.write("Hello World");
		br.write(" done ");
		i++;}
		br.close();
		OutputStream appendout1=fs.append(path,4192,null);//append


		line=" Second String ";
		path=new Path("/dir3/FileTest2.txt");
		System.out.println(" going to write");
		out=fs.create(path,FsPermission.getDefault(),false,(int)4192,(short)1,(long)4191,null);
		//OutputStream appendout=fs.append(path,4192,null);//append
		br = new BufferedWriter( new OutputStreamWriter( out, "UTF-8" ) );
		i=0;
		while(i<10000){
		br.write("HelloWorld");
		br.write(" done ");
		i++;}
		br.close();


		line=" Third String ";
		path=new Path("/dir3/FileTest3.txt");
		System.out.println(" going to write");
		out=fs.create(path,FsPermission.getDefault(),false,(int)4191,(short)1,(long)4192 ,null);
		//OutputStream appendout=fs.append(path,4192,null);//append
		br = new BufferedWriter( new OutputStreamWriter( out, "UTF-8" ) );
		i=0;
		while(i<1000){
		br.write("Hello World ");
		br.write(" done ");
		i++;}
		br.close();
		System.out.println(" going to read");

		FSDataInputStream in=fs.open(path,4191);
		//BufferedReader rd = new BufferedReader( new InputStreamReader( in, "UTF-8" ) );

		byte[] b1 = new byte[20000];
		int a=0;
		int count=0;
		while((a=in.read()) !=-1){
			count++;
		//int bytesRead = in.read(b1, 0, 10);
		//System.out.println(" Read String is "+(char)a);
		}
		System.out.println("Buf Size 1: Total Read Count "+count);
		in.seek(0);
		count=0;
		while((a=in.read(b1,0,10)) != -1){
			count+= a;
		}
		System.out.println("Buf Size 10: Total Read Count "+count);
		in.seek(0);
		count=0;
		while((a=in.read(b1,0,9)) != -1){
			count+= a;
		}
		System.out.println("Buf Size 9: Total Read Count "+count);
		in.seek(0);
		count=0;
		while((a=in.read(b1,0,1000)) != -1){
			count+= a;
		}
		System.out.println("Buf Size 1000: Total Read Count "+count);
		in.seek(0);
		count=0;
		while((a=in.read(b1,0,2000)) != -1){
			count+= a;
		}
		System.out.println("Buf Size 2000: Total Read Count "+count);
		in.seek(0);
		count=0;
		while((a=in.read(b1,0,5000)) != -1){
			count+= a;
		}
		System.out.println("Buf Size 5000: Total Read Count "+count);
		in.seek(0);
		count=0;
		while((a=in.read(b1,0,10000)) != -1){
			count+= a;
		}
		System.out.println("Buf Size 10000: Total Read Count "+count);


		in.seek(0);
		count=0;
		while((a=in.read(b1,0,20000)) != -1){
			count+= a;
		}
		System.out.println("Buf Size 20000: Total Read Count "+count);



		System.out.println(" Testing Seek and read count");
		in.seek(10);
		count=0;
		while((a=in.read(b1,0,10)) != -1){
			count+= a;
		}
		System.out.println("Buf Size 10: Total Read Count "+count);
		in.seek(9);
		count=0;
		while((a=in.read(b1,0,9)) != -1){
			count+= a;
		}
		System.out.println("Buf Size 9: Total Read Count "+count);
		in.seek(1000);
		count=0;
		while((a=in.read(b1,0,1000)) != -1){
			count+= a;
		}
		System.out.println("Buf Size 1000: Total Read Count "+count);
		in.seek(2000);
		count=0;
		while((a=in.read(b1,0,2000)) != -1){
			count+= a;
		}
		System.out.println("Buf Size 2000: Total Read Count "+count);
		in.seek(5000);
		count=0;
		while((a=in.read(b1,0,5000)) != -1){
			count+= a;
		}
		System.out.println("Buf Size 5000: Total Read Count "+count);
		in.seek(10000);
		count=0;
		while((a=in.read(b1,0,10000)) != -1){
			count+= a;
		}
		System.out.println("Buf Size 10000: Total Read Count "+count);


		//in.seek(20000); //outside fileLength
		count=0;
		while((a=in.read(b1,0,20000)) != -1){
			count+= a;
		}
		System.out.println("Buf Size 20000: Total Read Count "+count);//nothing to read

		System.out.println(" Append 1 Operation");

		OutputStream appendout2=fs.append(path,18,null);//append
		br = new BufferedWriter( new OutputStreamWriter( appendout2, "UTF-8" ) );
		i=0;
		//while(i<1000){
		br.write("First");
		//i++;}
		br.close();
		
		System.out.println(" Append 2 Operation");

		appendout2=fs.append(path,18,null);//append
		br = new BufferedWriter( new OutputStreamWriter( appendout2, "UTF-8" ) );
		i=0;
		//while(i<1000){
		br.write("Second");
		//i++;}
		br.close();

		FSDataInputStream in2=fs.open(path,4191);
		//BufferedReader rd = new BufferedReader( new InputStreamReader( in, "UTF-8" ) );

		byte[] b2 = new byte[20000];
		int a1=0;
		int count1=0;
		while((a1=in2.read()) !=-1){
			count1++;
		//int bytesRead = in.read(b1, 0, 10);
		//System.out.println(" Read String is "+(char)a);
		}
		System.out.println("Buf Size 1: Total Read Count "+count1);

		System.out.println(" Append 3 Operation");

		appendout2=fs.append(path,18,null);//append
		br = new BufferedWriter( new OutputStreamWriter( appendout2, "UTF-8" ) );
		i=0;
		//while(i<1000){
		br.write("Third");
		//i++;}
		br.close();

		in2=fs.open(path,4191);
		in2.seek(4321);
		//BufferedReader rd = new BufferedReader( new InputStreamReader( in, "UTF-8" ) );

		b2 = new byte[20000];
		a1=0;
		count1=0;
		while((a1=in2.read(b2,0,10000)) !=-1){
			count1 += a1;
		//int bytesRead = in.read(b1, 0, 10);
		//System.out.println(" Read String is "+(char)a);
		}
		System.out.println("Buf Size 10000: Total Read Count "+count1);

		System.out.println("Write Successful");
		fs.listStatus(new Path("/"));
		System.out.println(" ");
		fs.listStatus(new Path("/dir3"));
		System.out.println(" ");

		System.out.println(" Listing Tree");
		fs.listStatus(new Path("/"));
		System.out.println(" ");
		fs.listStatus(new Path("/dir3"));
		System.out.println(" ");
		System.out.println("Going to delete File");
		fs.delete(path,false);
		fs.delete(new Path("/dir3"),true);
		System.out.println(" ");
		System.out.println("delete Successful");
		fs.listStatus(new Path("/"));
		System.out.println(" ");
		fs.listStatus(new Path("/dir3"));
		System.out.println(" ");





	}

	static void printFileStatus (FileStatus fileStatus)throws IOException,UnsupportedEncodingException
	{
		ByteArrayOutputStream buffer = new ByteArrayOutputStream(64);
		DataInputBuffer in = new DataInputBuffer();

		buffer.reset();
		DataOutputStream out = new DataOutputStream(buffer);
		fileStatus.write(out);

		in.reset(buffer.toByteArray(), 0, buffer.size());

	}
}

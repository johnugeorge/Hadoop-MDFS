package org.apache.hadoop.mdfs.utils;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Comparator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;


public class DFSUtil {
	  /**
	   *    * Whether the pathname is valid.  Currently prohibits relative paths, 
	   *       * and names which contain a ":" or "/" 
	   *          */
	public static boolean isValidName(String src) {

		if (!src.startsWith(Path.SEPARATOR)) {
			return false;
		}
		StringTokenizer tokens = new StringTokenizer(src, Path.SEPARATOR);
		while(tokens.hasMoreTokens()) {
			String element = tokens.nextToken();
			if (element.equals("..") ||
					element.equals(".")  ||
					(element.indexOf(":") >= 0)  ||
					(element.indexOf("/") >= 0)) {
				return false;
					}
		}
		return true;
	}


	/**
	 *    * Converts a byte array to a string using UTF8 encoding.
	 *       */
	public static String bytes2String(byte[] bytes) {
		try {
			return new String(bytes, "UTF8");
		} catch(UnsupportedEncodingException e) {
			assert false : "UTF8 encoding is not supported ";
		}
		return null;
	}

	/**
	 *    * Converts a string to a byte array using UTF8 encoding.
	 *       */
	public static byte[] string2Bytes(String str) {
		try {
			return str.getBytes("UTF8");
		} catch(UnsupportedEncodingException e) {
			assert false : "UTF8 encoding is not supported ";
		}
		return null;
	}

}

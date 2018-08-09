package sdfs.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import sdfs.namenode.SDFSFileChannel;

public class Client {
	public static void main(String[] args) {
		int blockSize = 128 * 1024;
		SimpleDistributedFileSystem sdfs;
		String str1 = (args.length > 0) ? args[0] : "";
		String str2 = (args.length > 1) ? args[1] : "";
		String str3 = (args.length > 2) ? args[2] : "";
		String str4 = (args.length > 3) ? args[3] : "";
	
		// String str1 = "", str2 = "", str3 = "", str4 = "";

		// testcase0
		// str1="put";
		// str2="Readme-lab2.pdf";
		// str3="/a.pdf";

		// testcase1
		// str1 = "get";
		// str2 = "/a.pdf";
		// str3 = "src/get1.pdf";

		// testcase2
		// str1="mkdir";
		// str2="/test";
		// str3="";

		// testcase3
		// str1="mkdir";
		// str2="/test/test";
		// str3="";

		// testcase4
		// str1 = "put";
		// str2 = "Readme-lab2.pdf";
		// str3 = "/test/b.pdf";

		// testcase5
		// str1 = "get";
		// str2 = "/test/b.pdf";
		// str3 = "src/get2.pdf";

		// testcase6
		// str1 = "truncate";
		// str2 = "/a.pdf";
		// str3 = "5";

		// testcase7
		// str1 = "get";
		// str2 = "/a.pdf";
		// str3 = "src/get3.pdf";

		// testcase8
		// str1 = "append";
		// str2 = "Readme-lab2.pdf";
		// str3 = "/a.pdf";

		// testcase9
		// str1 = "get";
		// str2 = "/a.pdf";
		// str3 = "src/get4.pdf";

		// testcase10
		// str1 = "overwrite";
		// str2 = "Readme-lab2.pdf";
		// str3 = "/a.pdf";
		// str4 = "224190";

		// testcase11
		// str1 = "get";
		// str2 = "/a.pdf";
		// str3 = "src/get5.pdf";

		// testcase 12 cache
		// str1 = "testcache";
		 
		// test on my own computer
		InetSocketAddress nameNodeAddress = new InetSocketAddress("127.0.0.1", 4341);
		sdfs = new SimpleDistributedFileSystem(nameNodeAddress, 0);
		SDFSFileChannel sfc;
		FileInputStream fis;
		FileOutputStream fos;
		byte content[];
		int fileLength;
		try {
			if (str1.equals("put")) {
				File f = new File(str2);
				if (!f.exists()) {
					System.out.println("file doesn't exist");
				}
				fis = new FileInputStream(f);
				fileLength = (int) f.length();
				content = new byte[fileLength];
				fis.read(content);
				fis.close();
				sfc = sdfs.create(str3);
				ByteBuffer b = ByteBuffer.wrap(content);
				sfc.write(b);
				sfc.flush();
				sfc.close();
			} else if (str1.equals("get")) {
				File f = new File(str3);
				if (!f.exists()) {
					f.createNewFile();
				}
				fos = new FileOutputStream(f);
				sfc = sdfs.openReadonly(str2);
				ByteBuffer b = ByteBuffer.allocate((int) sfc.size());
				sfc.read(b);
				sfc.flush();
				sfc.close();
				content = b.array();
				fos.write(content);
				fos.close();
			} else if (str1.equals("mkdir")) {
				sdfs.mkdir(str2);
			} else if (str1.equals("append")) {
				File f = new File(str2);
				if (!f.exists()) {
					throw new IOException("file doesn't exist!");
				}
				fis = new FileInputStream(f);
				fileLength = (int) f.length();
				content = new byte[fileLength];
				fis.read(content);
				fis.close();
				sfc = sdfs.openReadWrite(str3);
				// get a bytebuffer
				ByteBuffer b = ByteBuffer.wrap(content);
				sfc.position(sfc.size());
				sfc.write(b);
				sfc.flush();
				sfc.close();
			} else if (str1.equals("overwrite")) {
				File f = new File(str2);
				if (!f.exists()) {
					throw new IOException("file doesn't exist!");
				}
				fis = new FileInputStream(f);
				fileLength = (int) f.length();
				content = new byte[fileLength];
				fis.read(content);
				fis.close();
				sfc = sdfs.openReadWrite(str3);
				// get a bytebuffer
				ByteBuffer b = ByteBuffer.wrap(content);
				sfc.position(Integer.parseInt(str4));
				sfc.write(b);
				sfc.flush();
				sfc.close();
			} else if (str1.equals("truncate")) {
				sfc = sdfs.openReadWrite(str2);
				sfc.truncate(Integer.parseInt(str3));
				sfc.flush();
				sfc.close();
			} else if (str1.equals("testcache")) {
				// put a file named c.pdf
				File f = new File("Readme-lab2.pdf");
				if (!f.exists()) {
					System.out.println("file doesn't exist");
				}
				fis = new FileInputStream(f);
				fileLength = (int) f.length();
				content = new byte[fileLength];
				fis.read(content);
				fis.close();
				sfc = sdfs.create("/dddddssssdddaklgsjlkjl.pdf");
				ByteBuffer b = ByteBuffer.wrap(content);
				sfc.write(b);

				// set position to 0
				sfc.position(0);

				// get a file named get6.pdf
				f = new File("src/get6.pdf");
				if (!f.exists()) {
					f.createNewFile();
				}
				fos = new FileOutputStream(f);
				b = ByteBuffer.allocate((int) sfc.size());
				sfc.read(b);
				content = b.array();
				fos.write(content);
				fos.close();

				// set position to 0
				sfc.position(0);

				// truncate
				sfc.truncate(1);

				// set position to 0
				sfc.position(0);

				// append
				f = new File("Readme-lab2.pdf");
				if (!f.exists()) {
					throw new IOException("file doesn't exist!");
				}
				fis = new FileInputStream(f);
				fileLength = (int) f.length();
				content = new byte[fileLength];
				fis.read(content);
				fis.close();
				// get a bytebuffer
				b = ByteBuffer.wrap(content);
				sfc.position(sfc.size());
				sfc.write(b);

				// set position to 0
				sfc.position(0);

				// get a file named get7.pdf
				f = new File("src/get7.pdf");
				if (!f.exists()) {
					f.createNewFile();
				}
				fos = new FileOutputStream(f);
				b = ByteBuffer.allocate((int) sfc.size());
				sfc.read(b);
				content = b.array();
				fos.write(content);
				fos.close();

				sfc.flush();
				sfc.close();
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

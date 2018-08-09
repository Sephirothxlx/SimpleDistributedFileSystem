/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.datanode;

import sdfs.protocol.IDataNodeProtocol;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

public class DataNode implements IDataNodeProtocol {
	/**
	 * The block size may be changed during test. So please use this constant.
	 */
	public static final int BLOCK_SIZE = 128 * 1024;
	public static final int DATA_NODE_PORT = 4342;
	public String dataNodepath = "dataNode/";
	// put off due to its difficulties
	// private final Map<UUID, Set<Integer>> uuidReadonlyPermissionCache = new
	// HashMap<>();
	// private final Map<UUID, Set<Integer>> uuidReadwritePermissionCache = new
	// HashMap<>();

	// datanode just read and write
	public DataNode() {
		File f = new File(dataNodepath);
		if (!f.exists())
			f.mkdir();
	}

	@Override
	public byte[] read(UUID fileUuid, int blockNumber, int offset, int size)
			throws IndexOutOfBoundsException, IOException {
		String filePath = dataNodepath + blockNumber + ".block";
		File f = new File(filePath);
		if (!f.exists()) {
			throw new IOException("file doesn't exist!");
		}
		if (offset < 0) {
			throw new IOException("offset is less than 0!");
		}
		if (offset + size > BLOCK_SIZE) {
			throw new IOException("offset is more than block size!");
		}
		byte[] result = new byte[size];
		FileInputStream in = new FileInputStream(f);
		in.skip(offset);
		// offset+size has been checked above
		in.read(result, 0, size);
		in.close();
		return result;
	}

	@Override
	public void write(UUID fileUuid, int blockNumber, int offset, byte[] b)
			throws IndexOutOfBoundsException, IOException {
		String filePath = dataNodepath + blockNumber + ".block";
		File f = new File(filePath);
		if (offset < 0) {
			throw new IOException("offset is less than 0!");
		}
		if (f.exists()) {
			FileOutputStream out = new FileOutputStream(f);
			out.write(b, offset, b.length - offset);
			out.close();
		} else {
			f.createNewFile();
			FileOutputStream out = new FileOutputStream(f);
			out.write(b, offset, b.length - offset);
			out.close();
		}
	}
}

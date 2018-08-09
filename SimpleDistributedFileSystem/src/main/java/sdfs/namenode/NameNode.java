/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.namenode;

import sdfs.filetree.BlockInfo;
import sdfs.filetree.DirNode;
import sdfs.filetree.Entry;
import sdfs.filetree.FileNode;
import sdfs.filetree.Node;
import sdfs.protocol.INameNodeDataNodeProtocol;
import sdfs.protocol.INameNodeProtocol;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.UUID;

public class NameNode implements INameNodeProtocol, INameNodeDataNodeProtocol {
	public static final int NAME_NODE_PORT = 4341;
	private final Map<UUID, FileNode> readonlyFile = new HashMap<>();
	private final Map<UUID, FileNode> readwriteFile = new HashMap<>();
	private final Map<UUID, DirNode> fileDir = new HashMap<>();
	private final Map<UUID, String> fileName = new HashMap<>();
	private String fileTreePath = "nameNode/filetree";
	private int maxBlockNumber;
	private DirNode root = null;
	private String dataNodeAddr = "127.0.0.1";
	private int dataNodePort = 4342;
	private int blockSize = 128 * 1024;

	// public NameNode(int nameNodePort) {
	//
	// }

	public NameNode() {
		File f = new File("nameNode");
		if (!f.exists())
			f.mkdir();
		f = new File(this.fileTreePath);
		if (!f.exists()) {
			try {
				f.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			this.root = new DirNode();
			this.updateFileTree();
		} else {
			this.root = loadFileTree(f);
			if (this.root == null) {
				System.out.println("file tree wrong!");
				System.exit(0);
			}
		}
		this.maxBlockNumber = getMaxBlockNumber();
	}

	public DirNode loadFileTree(File f) {
		DirNode root = null;
		try {
			ObjectInputStream in = new ObjectInputStream(new FileInputStream(f));
			root = (DirNode) in.readObject();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return root;
	}

	public int getMaxBlockNumber() {
		Stack<DirNode> s = new Stack<DirNode>();
		s.push(this.root);
		DirNode temp = null;
		int x = -1;
		while (!s.empty()) {
			temp = s.pop();
			Iterator<Entry> i = temp.iterator();
			Entry e = null;
			while (i.hasNext()) {
				e = i.next();
				if (e.getNode() instanceof DirNode) {
					s.push((DirNode) e.getNode());
				} else {
					FileNode fn = (FileNode) e.getNode();
					Iterator<BlockInfo> j = fn.iterator();
					BlockInfo bi = null;
					while (j.hasNext()) {
						bi = j.next();
						Iterator<LocatedBlock> k = bi.iterator();
						LocatedBlock lb = null;
						while (k.hasNext()) {
							lb = k.next();
							x = Math.max(x, lb.getBlockNumber());
						}
					}
				}
			}
		}
		return x;
	}

	public DirNode findDir(DirNode current, String dirPath) {
		String dir[] = dirPath.split("/");
		for (int i = 1; i < dir.length; i++) {
			if (current.find(dir[i], true) == null)
				return null;
			current = (DirNode) current.find(dir[i], true);
		}
		return current;
	}

	public void updateFileTree() {
		try {
			File file = new File(fileTreePath);
			if (!file.exists()) {
				file.createNewFile();
			}
			ObjectOutputStream oos = null;
			oos = new ObjectOutputStream(new FileOutputStream(file));
			oos.writeObject(this.root);
			oos.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("update fail");
		}
	}

	@Override
	public SDFSFileChannel openReadonly(String fileUri) throws IOException {
		int i = fileUri.lastIndexOf("/");
		if (i == -1)
			throw new IOException("wrong dir!");
		String dirPath = fileUri.substring(0, i);
		String fileName = fileUri.substring(i + 1, fileUri.length());
		DirNode dn = null;
		if (dirPath.length() == 0)
			dn = this.root;
		else
			dn = findDir(this.root, dirPath);
		if (dn == null) {
			throw new IOException("wrong dir!");
		}
		if (dn.find(fileName, false) == null) {
			throw new IOException("file not found!");
		}
		FileNode fn = (FileNode) dn.find(fileName, false);
		UUID uuid = UUID.randomUUID();
		this.readonlyFile.put(uuid, fn);
		this.fileName.put(uuid, fileName);
		SDFSFileChannel fc = new SDFSFileChannel(uuid, fn.getFileSize(), fn.getBlockAmount(), fn, true);
		return fc;
	}

	@Override
	public SDFSFileChannel openReadwrite(String fileUri)
			throws IndexOutOfBoundsException, IllegalStateException, IOException {
		int i = fileUri.lastIndexOf("/");
		if (i == -1)
			throw new IOException("wrong dir!");
		String dirPath = fileUri.substring(0, i);
		String fileName = fileUri.substring(i + 1, fileUri.length());
		DirNode dn = null;
		if (dirPath.length() == 0)
			dn = this.root;
		else
			dn = findDir(this.root, dirPath);
		if (dn == null) {
			throw new IOException("wrong dir!");
		}
		if (dn.find(fileName, false) == null) {
			throw new IOException("file not found!");
		}
		FileNode fn = (FileNode) dn.find(fileName, false);
		if (this.readwriteFile.containsValue(fn)) {
			throw new IOException("file is writing now!");
		}
		UUID uuid = UUID.randomUUID();
		this.readwriteFile.put(uuid, fn);
		this.fileDir.put(uuid, dn);
		this.fileName.put(uuid, fileName);
		SDFSFileChannel fc = new SDFSFileChannel(uuid, fn.getFileSize(), fn.getBlockAmount(), fn, false);
		return fc;
	}

	@Override
	public SDFSFileChannel create(String fileUri) throws IOException {
		int i = fileUri.lastIndexOf("/");
		if (i == -1)
			throw new IOException("wrong dir!");
		String dirPath = fileUri.substring(0, i);
		String fileName = fileUri.substring(i + 1, fileUri.length());
		DirNode dn = null;
		if (dirPath.length() == 0)
			dn = this.root;
		else
			dn = findDir(this.root, dirPath);
		if (dn == null) {
			throw new IOException("wrong dir!");
		}
		if (dn.find(fileName, false) != null) {
			throw new IOException("file exists!");
		}
		FileNode fn = new FileNode();
		UUID uuid = UUID.randomUUID();
		this.readwriteFile.put(uuid, fn);
		this.fileDir.put(uuid, dn);
		this.fileName.put(uuid, fileName);
		SDFSFileChannel fc = new SDFSFileChannel(uuid, fn.getFileSize(), fn.getBlockAmount(), fn, false);
		return fc;
	}

	@Override
	public void closeReadonlyFile(UUID fileUuid) throws IllegalStateException, IOException {
		if (this.readonlyFile.containsKey(fileUuid)) {
			this.readonlyFile.remove(fileUuid);
		} else {
			throw new IOException("wrong uuid!");
		}
	}

	@Override
	public void closeReadwriteFile(UUID fileUuid, int newFileSize)
			throws IllegalStateException, IllegalArgumentException, IOException {
		if (this.readwriteFile.containsKey(fileUuid)) {
			int blockAmount = this.readwriteFile.get(fileUuid).getBlockAmount();
			if (blockAmount * this.blockSize >= newFileSize && (blockAmount - 1) * this.blockSize < newFileSize) {
				FileNode fn = this.readwriteFile.get(fileUuid);
				fn.setFileSize(newFileSize);
				DirNode dn = this.fileDir.get(fileUuid);
				// if node exists, it must be overwrite or append
				// if node doesn't exists, it must be create
				Node n = dn.find(this.fileName.get(fileUuid), false);
				if (n == null) {
					dn.addEntry(new Entry(this.fileName.get(fileUuid), fn));
				} else {
					dn.updateEntry(this.fileName.get(fileUuid), fn);
				}
				updateFileTree();
				this.readwriteFile.remove(fileUuid);
				this.fileDir.remove(fileUuid);
			} else {
				throw new IOException("wrong new file size!");
			}
		} else {
			throw new IOException("wrong uuid!");
		}
	}

	@Override
	public void mkdir(String fileUri) throws IOException {
		int i = fileUri.lastIndexOf("/");
		if (i == -1)
			throw new IOException("wrong dir!");
		String dirPath = fileUri.substring(0, i);
		String dirName = fileUri.substring(i + 1, fileUri.length());
		DirNode dn = null;
		if (dirPath.length() == 0)
			dn = this.root;
		else
			dn = findDir(this.root, dirPath);
		if (dn == null) {
			throw new IOException("wrong dir!");
		}
		if (dn.find(dirName, true) != null) {
			throw new IOException("dir exists!");
		} else {
			DirNode d = new DirNode();
			dn.addEntry(new Entry(dirName, d));
			updateFileTree();
		}
	}

	@Override
	public LocatedBlock addBlock(UUID fileUuid) {
		if (this.readwriteFile.containsKey(fileUuid)) {
			FileNode fn = this.readwriteFile.get(fileUuid);
			InetSocketAddress address = new InetSocketAddress(dataNodeAddr, this.dataNodePort);
			this.maxBlockNumber++;
			LocatedBlock locatedBlock = new LocatedBlock(address, maxBlockNumber);
			BlockInfo blockInfo = new BlockInfo();
			blockInfo.addLocatedBlock(locatedBlock);
			fn.addBlockInfo(blockInfo);
			return locatedBlock;
		} else
			return null;
	}

	@Override
	public List<LocatedBlock> addBlocks(UUID fileUuid, int blockAmount) {
		ArrayList<LocatedBlock> blocks = new ArrayList<>();
		if (this.readwriteFile.containsKey(fileUuid)) {
			FileNode fn = this.readwriteFile.get(fileUuid);
			InetSocketAddress address = new InetSocketAddress(dataNodeAddr, this.dataNodePort);
			for (int i = 0; i < blockAmount; i++) {
				this.maxBlockNumber++;
				LocatedBlock locatedBlock = new LocatedBlock(address, this.maxBlockNumber);
				BlockInfo blockInfo = new BlockInfo();
				blockInfo.addLocatedBlock(locatedBlock);
				fn.addBlockInfo(blockInfo);
			}
			return blocks;
		} else
			return null;
	}

	@Override
	public void removeLastBlock(UUID fileUuid) throws IllegalStateException {
		if (this.readwriteFile.containsKey(fileUuid)) {
			this.readwriteFile.get(fileUuid).removeLastBlockInfo();
		} else {
			throw new IllegalStateException("remove fail!");
		}
	}

	@Override
	public void removeLastBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException {
		if (this.readwriteFile.containsKey(fileUuid)) {
			FileNode fn = this.readwriteFile.get(fileUuid);
			for (int i = 0; i < blockAmount; i++) {
				fn.removeLastBlockInfo();
			}
		} else {
			throw new IllegalStateException("remove fail!");
		}
	}
}

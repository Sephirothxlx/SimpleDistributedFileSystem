/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.namenode;

import sdfs.client.DataNodeStub;
import sdfs.client.NameNodeStub;
import sdfs.datanode.DataNode;
import sdfs.filetree.BlockInfo;
import sdfs.filetree.FileNode;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

public class SDFSFileChannel implements SeekableByteChannel, Flushable, Serializable {
	private static final long serialVersionUID = 6892411224902751501L;
	private final UUID uuid; // File uuid
	private int fileSize; // Size of this file
	private int blockAmount; // Total block amount of this file
	private final FileNode fileNode;
	private final boolean isReadOnly;
	private final Map<Integer, byte[]> dataBlocksCache = new HashMap<>();
	private boolean isOpen = true;
	private int position = 0;
	private int BLOCK_SIZE = 128 * 1024;
	private NameNodeStub nameNodeStub;
	
	//LRU and cache system
	private Map<Integer, CacheBlock> cacheBlocks = new HashMap<>();
	private ArrayList<Integer> LRU = new ArrayList<>();
	private int cacheSize = 4;

	SDFSFileChannel(UUID uuid, int fileSize, int blockAmount, FileNode fileNode, boolean isReadOnly) {
		this.uuid = uuid;
		this.fileSize = fileSize;
		this.blockAmount = blockAmount;
		this.fileNode = fileNode;
		this.isReadOnly = isReadOnly;
	}

	public void setNameNodeStub(NameNodeStub nameNodeStub) {
		this.nameNodeStub = nameNodeStub;
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {
		// todo your code here
		if (!this.isOpen)
			throw new ClosedChannelException();
		int readLength = 0;
		int totalLength = dst.capacity();
		// find the begin block index number
		// position is bytes number
		int beginIndex = this.position / this.BLOCK_SIZE;
		// to deal with two situations
		int x = ((this.position + totalLength) % this.BLOCK_SIZE == 0) ? -1 : 0;
		// index is from 0, maybe -1
		int endIndex = Math.min((this.position + totalLength) / this.BLOCK_SIZE + x, this.blockAmount - 1);
		int offset = this.position - beginIndex * this.BLOCK_SIZE;
		Iterator<BlockInfo> it0 = this.fileNode.iterator();
		// skip to the begin position
		for (int i = 0; i < beginIndex; i++) {
			it0.next();
		}
		BlockInfo bi;
		Iterator<LocatedBlock> it1;
		LocatedBlock lb;
		int temp;
		byte[] content;
		for (; beginIndex < endIndex + 1; beginIndex++) {
			bi = it0.next();
			it1 = bi.iterator();
			lb = it1.next();
			// the remain bytes may be smaller than a blocksize
			temp = (totalLength - readLength > this.BLOCK_SIZE) ? this.BLOCK_SIZE : totalLength - readLength;
			content = new byte[temp];
			content = readBlock(lb, offset, temp);
			dst.put(content);
			readLength += temp;
			this.position += readLength;
			// no need for offset from here
			offset = 0;
		}
		return readLength;
	}

	@Override
	public int write(ByteBuffer src) throws IOException {
		// todo your code here
		if (!isOpen)
			throw new ClosedChannelException();
		if (isReadOnly)
			throw new NonWritableChannelException();
		int totalLength = src.capacity();
		int writeLength = 0;
		// find the first block that needs to be write
		// position needs to be set if you want to write from some specific
		// position
		int beginIndex = this.position / this.BLOCK_SIZE;
		int x = ((this.position + totalLength) % this.BLOCK_SIZE == 0) ? -1 : 0;
		int endIndex = Math.min((this.position + totalLength) / this.BLOCK_SIZE + x, this.blockAmount - 1);
		// offset is for the first block to write
		// offset will be set to 0 after the first iteration
		int offset = this.position - beginIndex * this.BLOCK_SIZE;
		Iterator<BlockInfo> it0 = this.fileNode.iterator();
		BlockInfo bi;
		Iterator<LocatedBlock> it1;
		LocatedBlock lb;
		int temp;
		byte[] content;
		// when change happens less than blockAmount
		if (beginIndex < this.blockAmount) {
			for (int i = 0; i < beginIndex; i++) {
				it0.next();
			}
			for (; beginIndex < endIndex + 1; beginIndex++) {
				bi = it0.next();
				it1 = bi.iterator();
				lb = it1.next();
				temp = (totalLength - writeLength > this.BLOCK_SIZE) ? this.BLOCK_SIZE : totalLength - writeLength;
				content = new byte[temp];
				src.get(content);
				writeBlock(lb, content, offset);
				writeLength += temp;
				this.position += writeLength;
				offset = 0;
			}
			if (this.position > this.fileSize) {
				this.fileSize = this.position;
				this.fileNode.setFileSize(this.fileSize);
			}
		}
		// if there is need to add new blocks
		// writeLength < totalLength means there is still data to write but
		// blocks are not enough
		// like when create a file, these codes will be excuted
		if (writeLength < totalLength) {
			x = ((this.position + totalLength - writeLength) % this.BLOCK_SIZE == 0) ? -1 : 0;
			endIndex = (this.position + totalLength - writeLength) / this.BLOCK_SIZE + x;
			for (; beginIndex < endIndex + 1; beginIndex++) {
				temp = (totalLength - writeLength > this.BLOCK_SIZE) ? this.BLOCK_SIZE : totalLength - writeLength;
				content = new byte[temp];
				src.get(content);
				// need to ask for a new block
				lb = addBlock();
				writeBlock(lb, content, offset);
				writeLength += temp;
				this.fileSize += temp;
				this.fileNode.setFileSize(this.fileSize);
				this.position += writeLength;
				offset = 0;
			}
		}
		return writeLength;
	}

	@Override
	public long position() throws IOException {
		// todo your code here
		if (!this.isOpen)
			throw new ClosedChannelException();
		return this.position;
	}

	@Override
	public SeekableByteChannel position(long newPosition) throws IOException {
		// todo your code here
		if (!this.isOpen)
			throw new ClosedChannelException();
		if (newPosition < 0)
			throw new IOException("position is less than 0");
		else if (newPosition <= this.fileSize) {
			this.position = (int) newPosition;
		} else {
			this.position = this.fileSize;
			byte[] content = new byte[(int) (newPosition - this.fileSize)];
			ByteBuffer b = ByteBuffer.wrap(content);
			write(b);
		}
		return this;
	}

	@Override
	public long size() throws IOException {
		// todo your code here
		if (!this.isOpen)
			throw new ClosedChannelException();
		return this.fileSize;
	}

	@Override
	public SeekableByteChannel truncate(long size) throws IOException {
		// todo your code here
		if (!this.isOpen)
			throw new ClosedChannelException();
		if (isReadOnly)
			throw new NonWritableChannelException();
		// size: the part to keep
		// fileSize-size: the part to remove
		if (size < fileSize) {
			int x = (size % this.BLOCK_SIZE == 0) ? 0 : 1;
			// get the number of blocks to keep
			int amount = (int) (size / this.BLOCK_SIZE) + x;
			// need to remove the blockAmount - amount blocks
			if (amount < this.blockAmount) {
				try {
					removeLastBlocks(this.blockAmount - amount);
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			this.fileSize = (int) size;
			this.fileNode.setFileSize(this.fileSize);
			if (this.position > size) {
				this.position = (int) size;
			}
		} else {
			throw new IOException("new size is more than old size!");
		}
		return this;
	}

	public void removeLastBlocks(int number) throws IOException, ClassNotFoundException {
		for (int i = 0; i < number; i++) {
			// first, remove the filenode' blockinfo
			// second, remove the locatedblock in cache
			BlockInfo bi = this.fileNode.removeLastBlockInfo();
			// if there is no backup, then there will be one lb
			// the iterator will have only one entry
			Iterator it = bi.iterator();
			while (it.hasNext()) {
				LocatedBlock l = (LocatedBlock) it.next();
				if (this.cacheBlocks.containsKey(l.getBlockNumber())) {
					this.cacheBlocks.remove(l.getBlockNumber());
					this.LRU.remove(this.LRU.indexOf(l.getBlockNumber()));
				}
			}
			this.blockAmount--;
		}
		this.nameNodeStub.removeLastBlocks(this.uuid, number);
	}

	@Override
	public boolean isOpen() {
		// todo your code here
		return this.isOpen;
	}

	@Override
	public void close() throws IOException {
		// todo your code here
		if (this.isReadOnly) {
			this.nameNodeStub.closeReadonlyFile(uuid);
		} else {
			this.nameNodeStub.closeReadwriteFile(uuid, fileSize);
		}
		isOpen = false;
	}

	@Override
	public void flush() throws IOException {
		// todo your code here
		Iterator it = cacheBlocks.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry entry = (Map.Entry) it.next();
			CacheBlock cb = (CacheBlock) entry.getValue();
			if (cb.isDirty() == true) {
				writeBlock(cb.getLocatedBlock(), cb.getContent(), 0);
				DataNodeStub d = new DataNodeStub(cb.getLocatedBlock().getInetSocketAddress());
				d.write(uuid, cb.getLocatedBlock().getBlockNumber(), 0, cb.getContent());
				cb.setDirty(false);
			}
		}

	}

	// read from cache or locatedblock
	public byte[] readBlock(LocatedBlock block, int offset, int size) {
		byte content[];
		if (this.cacheBlocks.containsKey(block.getBlockNumber())) {
			content = this.cacheBlocks.get(block.getBlockNumber()).getContent();
			this.LRU.remove(this.LRU.indexOf(block.getBlockNumber()));
			this.LRU.add(0, block.getBlockNumber());
			return content;
		} else {
			DataNodeStub d = new DataNodeStub(block.getInetSocketAddress());
			try {
				content = d.read(this.uuid, block.getBlockNumber(), offset, size);
				if (this.cacheBlocks.size() >= this.cacheSize) {
					freeBlock();
				}
				this.cacheBlocks.put(block.getBlockNumber(), new CacheBlock(block, content, false));
				this.LRU.add(0, block.getBlockNumber());
				return content;
			} catch (IndexOutOfBoundsException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null;
	}

	public void writeBlock(LocatedBlock block, byte[] content, int offset) {
		if (!this.cacheBlocks.containsKey(block.getBlockNumber())) {
			// read this block into cache
			// for the next time read
			// read a total block
			readBlock(block, 0, this.BLOCK_SIZE);
		} else {
			this.LRU.remove(this.LRU.indexOf(block.getBlockNumber()));
			this.LRU.add(0, block.getBlockNumber());
		}
		// get the locatedblock
		CacheBlock c = this.cacheBlocks.get(block.getBlockNumber());
		byte[] old = c.getContent();
		int newSize = content.length + offset;
		newSize = Math.max(newSize, old.length);
		byte[] newBlock = new byte[newSize];
		System.arraycopy(old, 0, newBlock, 0, old.length);
		System.arraycopy(content, 0, newBlock, offset, content.length);
		c.setContent(newBlock);
		c.setDirty(true);
	}

	public LocatedBlock addBlock() {
		LocatedBlock lb = this.nameNodeStub.addBlock(this.uuid);
		if (this.cacheBlocks.size() >= this.cacheSize) {
			freeBlock();
		}
		byte[] content = new byte[0];
		this.LRU.add(0, lb.getBlockNumber());
		this.cacheBlocks.put(lb.getBlockNumber(), new CacheBlock(lb, content, true));
		BlockInfo bi = new BlockInfo();
		bi.addLocatedBlock(lb);
		this.fileNode.addBlockInfo(bi);
		this.blockAmount++;
		return lb;
	}

	//remove one cacheblock from cache
	private void freeBlock() {
		int index = this.LRU.remove(this.LRU.size() - 1);
		CacheBlock val = this.cacheBlocks.remove(index);
		if (val.isDirty()) {
			DataNodeStub d = new DataNodeStub(val.getLocatedBlock().getInetSocketAddress());
			try {
				d.write(this.uuid, val.getLocatedBlock().getBlockNumber(), 0, val.getContent());
			} catch (IndexOutOfBoundsException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}

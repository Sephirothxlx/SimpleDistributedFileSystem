/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.message.AbstractMessage;
import sdfs.message.NameNodeRequest;
import sdfs.message.NameNodeResponse;
import sdfs.namenode.LocatedBlock;
import sdfs.namenode.SDFSFileChannel;
import sdfs.protocol.INameNodeProtocol;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.UUID;

public class NameNodeStub implements INameNodeProtocol {
	private Socket socket = null;
	private InetSocketAddress address = null;

	public NameNodeStub(InetSocketAddress nameNodeAddr) {
		this.address = nameNodeAddr;
	}

	//stub's duty is to make request and deal response
	@Override
	public SDFSFileChannel openReadonly(String fileUri) throws IOException {
		this.socket = new Socket();
		this.socket.connect(this.address);
		ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
		oos.writeObject(new NameNodeRequest(AbstractMessage.Type.OPEN_R_REQUEST, fileUri, null, 0));
		oos.flush();
		ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
		try {
			NameNodeResponse nameNodeResponse = (NameNodeResponse) ois.readObject();
			ois.close();
			if (nameNodeResponse.getType() == AbstractMessage.Type.OPEN_R_RESPONSE) {
				SDFSFileChannel s = nameNodeResponse.getSdfsFileChannel();
				s.setNameNodeStub(this);
				return s;
			} else if (nameNodeResponse.getType() == AbstractMessage.Type.ERROR) {
				throw new IOException("open fail");
			} else {
				throw new IOException("wrong type");
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public SDFSFileChannel openReadwrite(String fileUri)
			throws IndexOutOfBoundsException, IllegalStateException, IOException {
		this.socket = new Socket();
		this.socket.connect(this.address);
		ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
		oos.writeObject(new NameNodeRequest(AbstractMessage.Type.OPEN_RW_REQUEST, fileUri, null, 0));
		oos.flush();
		ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
		try {
			NameNodeResponse nameNodeResponse = (NameNodeResponse) ois.readObject();
			ois.close();
			if (nameNodeResponse.getType() == AbstractMessage.Type.OPEN_RW_RESPONSE) {
				SDFSFileChannel s = nameNodeResponse.getSdfsFileChannel();
				s.setNameNodeStub(this);
				return s;
			} else if (nameNodeResponse.getType() == AbstractMessage.Type.ERROR) {
				throw new IOException("open fail");
			} else {
				throw new IOException("wrong type");
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public SDFSFileChannel create(String fileUri) throws IllegalStateException, IOException {
		this.socket = new Socket();
		this.socket.connect(this.address);
		ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
		oos.writeObject(new NameNodeRequest(AbstractMessage.Type.CREATE_REQUEST, fileUri, null, 0));
		oos.flush();
		ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
		try {
			NameNodeResponse nameNodeResponse = (NameNodeResponse) ois.readObject();
			ois.close();
			if (nameNodeResponse.getType() == AbstractMessage.Type.CREATE_RESPONSE) {
				SDFSFileChannel s = nameNodeResponse.getSdfsFileChannel();
				s.setNameNodeStub(this);
				return s;
			} else if (nameNodeResponse.getType() == AbstractMessage.Type.ERROR) {
				throw new IOException("create fail");
			} else {
				throw new IOException("wrong type");
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public void closeReadonlyFile(UUID fileUuid) throws IllegalStateException, IOException {
		this.socket = new Socket();
		this.socket.connect(this.address);
		ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
		oos.writeObject(new NameNodeRequest(AbstractMessage.Type.CLOSE_R_REQUEST, null, fileUuid, 0));
		oos.flush();
		ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
		try {
			NameNodeResponse nameNodeResponse = (NameNodeResponse) ois.readObject();
			ois.close();
			if (nameNodeResponse.getType() == AbstractMessage.Type.ERROR) {
				throw new IOException("close fail");
			} else if (nameNodeResponse.getType() != AbstractMessage.Type.CLOSE_R_RESPONSE) {
				throw new IOException("wrong type");
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void closeReadwriteFile(UUID fileUuid, int newFileSize)
			throws IllegalStateException, IllegalArgumentException, IOException {
		this.socket = new Socket();
		this.socket.connect(this.address);
		ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
		oos.writeObject(new NameNodeRequest(AbstractMessage.Type.CLOSE_RW_REQUEST, null, fileUuid, newFileSize));
		oos.flush();
		ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
		try {
			NameNodeResponse nameNodeResponse = (NameNodeResponse) ois.readObject();
			ois.close();
			if (nameNodeResponse.getType() == AbstractMessage.Type.ERROR) {
				throw new IOException("close fail");
			} else if (nameNodeResponse.getType() != AbstractMessage.Type.CLOSE_RW_RESPONSE) {
				throw new IOException("wrong type");
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void mkdir(String fileUri) throws IOException {
		this.socket = new Socket();
		this.socket.connect(this.address);
		ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
		oos.writeObject(new NameNodeRequest(AbstractMessage.Type.MKDIR_REQUEST, fileUri, null, 0));
		oos.flush();
		ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
		try {
			NameNodeResponse nameNodeResponse = (NameNodeResponse) ois.readObject();
			ois.close();
			if (nameNodeResponse.getType() == AbstractMessage.Type.ERROR) {
				throw new IOException("mkdir fail");
			} else if (nameNodeResponse.getType() != AbstractMessage.Type.MKDIR_RESPONSE) {
				throw new IOException("wrong type");
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public LocatedBlock addBlock(UUID fileUuid) {
		try {
			this.socket = new Socket();
			this.socket.connect(this.address);
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			oos.writeObject(new NameNodeRequest(AbstractMessage.Type.ADDBLOCK_REQUEST, null, fileUuid, 0));
			oos.flush();
			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
			NameNodeResponse nameNodeResponse = (NameNodeResponse) ois.readObject();
			ois.close();
			if (nameNodeResponse.getType() == AbstractMessage.Type.ADDBLOCK_RESPONSE) {
				return nameNodeResponse.getLocatedBlock();
			} else
				return null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public List<LocatedBlock> addBlocks(UUID fileUuid, int blockAmount) {
		try {
			this.socket = new Socket();
			this.socket.connect(this.address);
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			oos.writeObject(new NameNodeRequest(AbstractMessage.Type.ADDBLOCKS_REQUEST, null, fileUuid, 0));
			oos.flush();
			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
			NameNodeResponse nameNodeResponse = (NameNodeResponse) ois.readObject();
			ois.close();
			if (nameNodeResponse.getType() == AbstractMessage.Type.ADDBLOCKS_RESPONSE) {
				return nameNodeResponse.getBlocks();
			} else
				return null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public void removeLastBlock(UUID fileUuid) throws IllegalStateException {
		try {
			this.socket = new Socket();
			this.socket.connect(this.address);
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			oos.writeObject(new NameNodeRequest(AbstractMessage.Type.REMOVE_BLOCK_REQUEST, null, fileUuid, 0));
			oos.flush();
			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
			NameNodeResponse nameNodeResponse = (NameNodeResponse) ois.readObject();
			ois.close();
			if (nameNodeResponse.getType() != AbstractMessage.Type.REMOVE_BLOCK_RESPONSE) {
				System.out.println("remove fail");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void removeLastBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException {
		try {
			this.socket = new Socket();
			this.socket.connect(this.address);
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			oos.writeObject(new NameNodeRequest(AbstractMessage.Type.REMOVE_BLOCKS_REQUEST, null, fileUuid, blockAmount));
			oos.flush();
			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
			NameNodeResponse nameNodeResponse = (NameNodeResponse) ois.readObject();
			ois.close();
			if (nameNodeResponse.getType() != AbstractMessage.Type.REMOVE_BLOCKS_RESPONSE) {
				System.out.println("remove fail");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

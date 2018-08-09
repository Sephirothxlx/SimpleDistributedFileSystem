/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.message.AbstractMessage;
import sdfs.message.DataNodeRequest;
import sdfs.message.DataNodeResponse;
import sdfs.protocol.IDataNodeProtocol;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.UUID;

public class DataNodeStub implements IDataNodeProtocol {
	private Socket s = null;
	private InetSocketAddress dataNodeAddress = null;

	//the InetSocketAddress is assigned by upper user
	public DataNodeStub(InetSocketAddress dataNodeAddress) {
		this.dataNodeAddress = dataNodeAddress;
		this.s = new Socket();
	}

	@Override
	public byte[] read(UUID fileUuid, int blockNumber, int offset, int size)
			throws IndexOutOfBoundsException, IOException {
		s.connect(this.dataNodeAddress);
		ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
		DataNodeRequest req = new DataNodeRequest(AbstractMessage.Type.READ_BLOCK_REQUEST,fileUuid, blockNumber, offset, size,null);
		out.writeObject(req);
		out.flush();
		//get back information
		ObjectInputStream in = new ObjectInputStream(s.getInputStream());
		DataNodeResponse res=null;
		try {
			res = (DataNodeResponse) in.readObject();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		in.close();
		if (res.getType() == AbstractMessage.Type.READ_BLOCK_RESPONSE) {
			return res.getByte();
		} else if (res.getType() == AbstractMessage.Type.READ_BLOCK_FAIL) {
			throw new IOException("reading blocks fails!");
		} else {
			throw new IOException("incorrect response type!");
		}
	}

	@Override
	public void write(UUID fileUuid, int blockNumber, int offset, byte[] b)
			throws IndexOutOfBoundsException, IOException {
		s.connect(this.dataNodeAddress);
		ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
		DataNodeRequest req = new DataNodeRequest(AbstractMessage.Type.WRITE_BLOCK_REQUEST,fileUuid, blockNumber, offset, 0,b);
		out.writeObject(req);
		out.flush();
		ObjectInputStream in = new ObjectInputStream(s.getInputStream());
		DataNodeResponse res=null;
		try {
			res = (DataNodeResponse) in.readObject();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		in.close();
		if (res.getType() == AbstractMessage.Type.WRITE_BLOCK_FAIL) {
			throw new IOException("writing blocks fails!");
		} else if (res.getType() != AbstractMessage.Type.WRITE_BLOCK_RESPONSE) {
			throw new IOException("incorrect response type!");
		} 
	}
}

package sdfs.namenode;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import sdfs.message.AbstractMessage;
import sdfs.message.NameNodeRequest;
import sdfs.message.NameNodeResponse;

public class NameNodeServer {
	private NameNode nameNode;
	private ServerSocket serverSocket;

	private void sendResponse(NameNodeResponse nameNodeResponse, Socket socket) {
		ObjectOutputStream oos = null;
		try {
			oos = new ObjectOutputStream(socket.getOutputStream());
			oos.writeObject(nameNodeResponse);
			oos.flush();
			oos.close();
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		NameNodeServer s = new NameNodeServer();
		s.start();
	}

	public void start() {
		try {
			this.serverSocket = new ServerSocket(NameNode.NAME_NODE_PORT);
			this.nameNode = new NameNode();
			Socket socket;
			while (true) {
				socket = serverSocket.accept();
				ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
				NameNodeRequest request = (NameNodeRequest) ois.readObject();
				handleRequest(request, socket);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void handleRequest(NameNodeRequest request, Socket socket) {
		SDFSFileChannel sdfsFileChannel;
		try {
			switch (request.getType()) {
			case OPEN_R_REQUEST:
				sdfsFileChannel = nameNode.openReadonly(request.getFileUri());
				sendResponse(
						new NameNodeResponse(AbstractMessage.Type.OPEN_R_RESPONSE, sdfsFileChannel, null, null, null),
						socket);
				break;
			case OPEN_RW_REQUEST:
				sdfsFileChannel = nameNode.openReadwrite(request.getFileUri());
				sendResponse(
						new NameNodeResponse(AbstractMessage.Type.OPEN_RW_RESPONSE, sdfsFileChannel, null, null, null),
						socket);
				break;
			case CREATE_REQUEST:
				sdfsFileChannel = nameNode.create(request.getFileUri());
				sendResponse(
						new NameNodeResponse(AbstractMessage.Type.CREATE_RESPONSE, sdfsFileChannel, null, null, null),
						socket);
				break;
			case CLOSE_R_REQUEST:
				nameNode.closeReadonlyFile(request.getFileUUID());
				sendResponse(new NameNodeResponse(AbstractMessage.Type.CLOSE_R_RESPONSE, null, null, null, null),
						socket);
				break;
			case CLOSE_RW_REQUEST:
				nameNode.closeReadwriteFile(request.getFileUUID(), request.getNumber());
				sendResponse(new NameNodeResponse(AbstractMessage.Type.CLOSE_RW_RESPONSE, null, null, null, null),
						socket);
				break;
			case MKDIR_REQUEST:
				nameNode.mkdir(request.getFileUri());
				sendResponse(new NameNodeResponse(AbstractMessage.Type.MKDIR_RESPONSE, null, null, null, null), socket);
				break;
			case ADDBLOCK_REQUEST:
				LocatedBlock block = nameNode.addBlock(request.getFileUUID());
				sendResponse(new NameNodeResponse(AbstractMessage.Type.ADDBLOCK_RESPONSE, null, block, null, null),
						socket);
				break;
			case ADDBLOCKS_REQUEST:
				List<LocatedBlock> blocks = nameNode.addBlocks(request.getFileUUID(), request.getNumber());
				sendResponse(new NameNodeResponse(AbstractMessage.Type.ADDBLOCKS_RESPONSE, null, null, blocks, null),
						socket);
				break;
			case REMOVE_BLOCK_REQUEST:
				nameNode.removeLastBlock(request.getFileUUID());
				sendResponse(new NameNodeResponse(AbstractMessage.Type.REMOVE_BLOCK_RESPONSE, null, null, null, null),
						socket);
				break;
			case REMOVE_BLOCKS_REQUEST:
				nameNode.removeLastBlocks(request.getFileUUID(), request.getNumber());
				sendResponse(new NameNodeResponse(AbstractMessage.Type.REMOVE_BLOCKS_RESPONSE, null, null, null, null),
						socket);
				break;
			default:
				break;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

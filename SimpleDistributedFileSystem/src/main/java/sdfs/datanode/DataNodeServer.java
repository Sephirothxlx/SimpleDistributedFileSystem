package sdfs.datanode;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import sdfs.message.DataNodeRequest;
import sdfs.message.DataNodeResponse;
import sdfs.message.AbstractMessage;

public class DataNodeServer {
	private DataNode dataNode;
	private Socket socket;

	public static void main(String[] args) {
		DataNodeServer server = new DataNodeServer();
		server.start();
	}

	public void start() {
		try {
			ServerSocket serverSocket = new ServerSocket(DataNode.DATA_NODE_PORT);
			dataNode = new DataNode();
			//handle requests from port 4342
			while (true) {
				socket = serverSocket.accept();
				ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
				DataNodeRequest request = (DataNodeRequest) ois.readObject();
				handleRequest(request);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void handleRequest(DataNodeRequest request) {
		try {
			//there should be just two kinds of requests
			if (request.getType() == AbstractMessage.Type.READ_BLOCK_REQUEST) {
				//real operation
				byte b[] = dataNode.read(request.getUUID(), request.getBlockNumber(), request.getOffset(), request.getSize());
				//make response
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				oos.writeObject(new DataNodeResponse(AbstractMessage.Type.READ_BLOCK_RESPONSE, b));
				oos.flush();
				oos.close();
				socket.close();
			} else if (request.getType() == AbstractMessage.Type.WRITE_BLOCK_REQUEST) {
				//real operation
				dataNode.write(request.getUUID(), request.getBlockNumber(), request.getOffset(), request.getByte());
                //make response
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                oos.writeObject(new DataNodeResponse(AbstractMessage.Type.WRITE_BLOCK_RESPONSE,null));
                oos.flush();
                oos.close();
                socket.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
			// should be some errors information return
		}
	}

}

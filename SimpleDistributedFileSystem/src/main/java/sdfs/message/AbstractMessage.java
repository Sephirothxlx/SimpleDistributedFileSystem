package sdfs.message;

import java.io.Serializable;

public class AbstractMessage implements Serializable {
	public enum Type implements Serializable {
		// maybe some kinds of type not used
		READ_BLOCK_REQUEST, READ_BLOCK_RESPONSE, WRITE_BLOCK_RESPONSE, WRITE_BLOCK_FAIL, READ_BLOCK_FAIL, WRITE_BLOCK_REQUEST, OPEN_R_REQUEST, OPEN_R_RESPONSE, OPEN_RW_REQUEST, OPEN_RW_RESPONSE, CREATE_RESPONSE, CREATE_REQUEST, CLOSE_R_REQUEST, CLOSE_R_RESPONSE, CLOSE_RW_RESPONSE, CLOSE_RW_REQUEST, MKDIR_REQUEST, MKDIR_RESPONSE, ADDBLOCK_RESPONSE, ADDBLOCK_REQUEST, ADDBLOCKS_RESPONSE, ADDBLOCKS_REQUEST, REMOVE_BLOCK_RESPONSE, REMOVE_BLOCK_REQUEST, REMOVE_BLOCKS_REQUEST, REMOVE_BLOCKS_RESPONSE, LS_REQUEST, LS_RESPONSE, ERROR
	}

	private Type type;

	public AbstractMessage() {

	}

	public AbstractMessage(Type type) {
		this.type = type;
	}

	public Type getType() {
		return this.type;
	}
}

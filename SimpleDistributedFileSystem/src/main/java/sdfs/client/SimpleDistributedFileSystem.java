/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.namenode.SDFSFileChannel;

import java.io.IOException;
import java.net.InetSocketAddress;

public class SimpleDistributedFileSystem implements ISimpleDistributedFileSystem {
    /**
     * @param fileDataBlockCacheSize Buffer size for file data block. By default, it should be 16.
     *                               That means 16 block of data will be cache on local.
     *                               And you should use LRU algorithm to replace it.
     *                               It may be change during test. So don't assert it will equal to a constant.
     */
	private NameNodeStub nameNodeStub;
	
	//just use the functions in nameNodeStub
    public SimpleDistributedFileSystem(InetSocketAddress nameNodeAddress, int fileDataBlockCacheSize) {
        //todo your code here
    	this.nameNodeStub = new NameNodeStub(nameNodeAddress);
    }

    @Override
    public SDFSFileChannel openReadonly(String fileUri) throws IOException {
        //todo your code here
    	SDFSFileChannel s = this.nameNodeStub.openReadonly(fileUri);
        return s;
    }

    @Override
    public SDFSFileChannel create(String fileUri) throws IOException {
        //todo your code here
    	SDFSFileChannel s =this. nameNodeStub.create(fileUri);
        return s;
    }

    @Override
    public SDFSFileChannel openReadWrite(String fileUri) throws IOException {
        //todo your code here
    	SDFSFileChannel s = this.nameNodeStub.openReadwrite(fileUri);
        return s;
    }

    @Override
    public void mkdir(String fileUri) throws IOException {
        //todo your code here
    	this.nameNodeStub.mkdir(fileUri);
    }
}

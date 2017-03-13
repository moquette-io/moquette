package io.moquette.spi.persistence.lmdb;

import java.nio.ByteBuffer;
import java.util.AbstractQueue;
import java.util.Collections;
import java.util.Iterator;

import org.lmdbjava.Cursor;
import org.lmdbjava.CursorIterator;
import org.lmdbjava.CursorIterator.KeyVal;
import org.lmdbjava.Dbi;
import org.lmdbjava.GetOp;
import org.lmdbjava.SeekOp;
import org.lmdbjava.Txn;

import io.moquette.spi.IMessagesStore;
import io.moquette.spi.IMessagesStore.StoredMessage;

public class LMDBQueue extends AbstractQueue<IMessagesStore.StoredMessage> {

    private final ByteBuffer clientID;
    private final Dbi<ByteBuffer> clientQueue;
    private final AbstractLMDBStore store;
    
    public LMDBQueue(String clientID, Dbi<ByteBuffer> clientQueue, AbstractLMDBStore store) {
        this.clientID = store.createKey(clientID);
        this.clientQueue = clientQueue;
        this.store = store;
    }

    @Override
    public boolean offer(IMessagesStore.StoredMessage e) {
        try (Txn<ByteBuffer> txn = store.getEnv().txnWrite();) {
            this.clientQueue.put(store.createKey(e.getClientID()), store.createValue(e));
            return true;
        }
    }

    @Override
    public IMessagesStore.StoredMessage poll() {
        try (Txn<ByteBuffer> txn = store.getEnv().txnWrite(); final Cursor<ByteBuffer> c = this.clientQueue.openCursor(txn);) {
            boolean contains = c.get(clientID, GetOp.MDB_SET_KEY);
            if(!contains)
                return null;
            else {
                IMessagesStore.StoredMessage val = store.decodeValue(c.val());
                c.delete();
                return val;
            }
                
        }
    }

    @Override
    public IMessagesStore.StoredMessage peek() {
        try (Txn<ByteBuffer> txn = store.getEnv().txnRead(); final Cursor<ByteBuffer> c = this.clientQueue.openCursor(txn);) {
            boolean contains = c.get(clientID, GetOp.MDB_SET_KEY);
            if(!contains)
                return null;
            else {
                IMessagesStore.StoredMessage val = store.decodeValue(c.val());
                return val;
            }                
        }
    }

    @Override
    public Iterator<IMessagesStore.StoredMessage> iterator() {
        try (Txn<ByteBuffer> txn = store.getEnv().txnRead(); final Cursor<ByteBuffer> c = this.clientQueue.openCursor(txn);) {
            boolean contains = c.get(clientID, GetOp.MDB_SET_KEY);
            if(!contains)
                return Collections.emptyIterator();
            else
                return new Iterator<IMessagesStore.StoredMessage>() {
                    
                    int currentPosition = 0;
                
                    @Override
                    public boolean hasNext() {        
                        long currentCount = c.count();
                        return currentPosition < currentCount;
                    }
    
                    @Override
                    public StoredMessage next() {
                        boolean found = c.seek(SeekOp.MDB_NEXT_DUP);
                        if(found) {
                            currentPosition+=1;
                            return (IMessagesStore.StoredMessage) store.decodeValue(c.val());
                        } else {
                            return null;
                        }
                    }
                }; 
        }
    }

    @Override
    public int size() {
        try (Txn<ByteBuffer> txn = store.getEnv().txnRead(); final Cursor<ByteBuffer> c = this.clientQueue.openCursor(txn);) {
            boolean contains = c.get(clientID, GetOp.MDB_SET_KEY);
            if(!contains)
                return 0;
            else
                return (int) c.count();
        }
    }
    
}

package dkvs;

import dkvs.OperationLogger.OperationLogger;
import dkvs.kvsservice.KeyValueStorage;
import dkvs.sockets.SocketHandler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by amir.
 */
public class BackupServer implements Runnable {
    public enum Status {
        NORMAL, VIEW_CHANGE, RECOVERING
    }
    private Status status;
    private int replicaNumber;
    private int currentView;
    private LinkedBlockingQueue<Message> queue;//main queue, where all messages come
    private Map<Integer, SocketHandler> serverList;
    private OperationLogger logger;
    private KeyValueStorage storage;


    public BackupServer(int replicaNumber, LinkedBlockingQueue<Message> queue,
                        Map<Integer, SocketHandler> serverList) {
        this.serverList = serverList;
        this.replicaNumber = replicaNumber;
        this.queue = queue;
        this.storage = new KeyValueStorage();
        //automatically loads log from file and commit operations to storage
        this.logger = new OperationLogger(replicaNumber, storage);
        //???
        this.status = Status.RECOVERING;
    }

    @Override
    public void run() {
        while (true) {
            Message curMessage;
            try {
                curMessage = queue.take();
            } catch (InterruptedException e) {
                //
                return;
            }
            switch (curMessage.type) {
                //case GET :

                //    break;

            }
        }
    }

}

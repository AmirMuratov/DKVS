package dkvs;

import dkvs.OperationLogger.Operation;
import dkvs.OperationLogger.OperationLogger;
import dkvs.kvsservice.KeyValueStorage;
import dkvs.sockets.SocketHandler;

import java.util.Map;
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
    private LinkedBlockingQueue<InputMessage> queue;//main queue, where all messages come
    private SocketHandler[] serverList;//serverNumber -> socket that handles connection to this server
    private Map<Integer, Integer> lastMessage;//last message time from i-th Server
    private OperationLogger logger;
    private KeyValueStorage service;
    //private Map<    > currentOperations;


    public BackupServer(int replicaNumber, LinkedBlockingQueue<InputMessage> queue,
                        SocketHandler[] serverList) {
        this.serverList = serverList;
        this.replicaNumber = replicaNumber;
        this.queue = queue;
        this.service = new KeyValueStorage();
        //automatically loads log from file and commit operations to storage
        this.logger = new OperationLogger(replicaNumber, service);
        //?????
        this.status = Status.NORMAL;
    }

    private int getPrimary() {
        return currentView % Configuration.getNumberOfServers() + 1;
    }

    @Override
    public void run() {
        while (true) {
            InputMessage curMessage;
            try {
                curMessage = queue.take();
            } catch (InterruptedException e) {
                return;
            }
            switch (status) {
                case NORMAL:
                    if (curMessage.socketHandler.getServerNum() == getPrimary()) {
                        logger.commitAll();
                    }
                    switch (curMessage.type) {
                        case PONG:
                            continue;
                        case PING:
                            curMessage.socketHandler.sendMessage(new OutputMessage("PONG"));
                            continue;
                        case GET:
                            String key = curMessage.key;
                            String value = service.get(key);
                            if (value != null) {
                                curMessage.socketHandler.sendMessage(new OutputMessage("VALUE " + key + " " + value));
                            } else {
                                curMessage.socketHandler.sendMessage(new OutputMessage("NOT_FOUND"));
                            }
                            continue;
                        case PREPARE:
                            //PREPARE replica_number view_number operation_number (DELETE key | SET key value)
                            Operation op = curMessage.op;
                            logger.addOperation(op);
                            curMessage.socketHandler.sendMessage(new OutputMessage("PREPARE_OK " + curMessage.operationNumber));
                        case SET:

                            continue;
                        case DELETE:

                            continue;
                        default:
                    }
                case RECOVERING:
                case VIEW_CHANGE:
            }
        }
    }
}

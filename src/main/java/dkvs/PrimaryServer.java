package dkvs;

import dkvs.operationlogger.Operation;
import dkvs.operationlogger.OperationLogger;
import dkvs.kvsservice.KeyValueStorage;
import dkvs.sockets.ServerSocketListener;
import dkvs.sockets.SocketHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by amir.
 */
public class PrimaryServer implements Runnable {
    private int replicaNumber;
    private int view;
    private BlockingQueue<InputMessage> queue;
    private SocketHandler[] serverList;
    private int curOperationNumber;
    private KeyValueStorage service;
    private OperationLogger logger;
    private Map<Integer, Integer> currentOperations; //operationNumber -> number of received prepare_ok's
    private Map<Integer, InputMessage> clientMessages; //operationNumber -> message from client

    public PrimaryServer(int replicaNumber, ServerSocketListener listener) {
        this.currentOperations = new HashMap<>();
        this.clientMessages = new HashMap<>();
        this.serverList = listener.getServerList();
        this.replicaNumber = replicaNumber;
        this.queue = listener.getInputQueue();
        this.service = new KeyValueStorage();
        curOperationNumber = 0;
        this.logger = new OperationLogger(replicaNumber, service);
        this.view = 0;
    }

    public PrimaryServer(int replicaNumber, OperationLogger logger, ServerSocketListener listener, int view) {
        this.currentOperations = new HashMap<>();
        this.clientMessages = new HashMap<>();
        this.serverList = listener.getServerList();
        this.replicaNumber = replicaNumber;
        this.queue = listener.getInputQueue();
        this.logger = logger;
        this.service = logger.getService();
        curOperationNumber = logger.getLastPrepared() + 1;
        this.view = view;
    }

    @Override
    public void run() {
        while (true) {
            InputMessage curMessage;
            try {
                curMessage = queue.take();
            } catch (InterruptedException e) {
                break;
            }
            switch (curMessage.type) {
                case GET:
                    String key = curMessage.key;
                    String value = service.get(key);
                    if (value != null) {
                        curMessage.socketHandler.sendMessage(new OutputMessage("VALUE " + key + " " + value));
                    } else {
                        curMessage.socketHandler.sendMessage(new OutputMessage("NOT_FOUND"));
                    }
                    continue;
                case SET:
                case DELETE:
                    curOperationNumber++;
                    logger.addOperation(curMessage.op);
                    String prepare = curMessage.type == MessageType.SET ? "PREPARE " + view +
                            " " + curOperationNumber + " SET " +
                            curMessage.op.getKey() + " " + curMessage.op.getValue() :
                            "PREPARE " + view + " " + curOperationNumber + " DELETE " +
                                    curMessage.op.getKey();
                    for (SocketHandler handler : serverList) {
                        if (handler != null) {
                            handler.sendMessage(new OutputMessage(prepare));
                        }
                    }
                    currentOperations.put(curOperationNumber, 1);
                    clientMessages.put(curOperationNumber, curMessage);
                    continue;
                case PREPARE_OK:
                    int operationNumber = curMessage.operationNumber;
                    if (!currentOperations.containsKey(operationNumber)) {
                        continue;
                    }
                    currentOperations.put(operationNumber, currentOperations.get(operationNumber) + 1);
                    if (currentOperations.get(operationNumber) >= Configuration.getNumberOfServers() / 2 + 1) {
                        //if we received >= f + 1 prepare_ok's for certain operation,
                        // we sholud commit this operation, send answer to client answer
                        // remove this operation from maps.
                        logger.commitAllUntil(operationNumber);
                        InputMessage clientsMessage = clientMessages.get(operationNumber);
                        for (SocketHandler handler : serverList) {
                            if (handler != null) {
                                handler.sendMessage(new OutputMessage("COMMIT " + operationNumber));
                            }
                        }
                        switch (clientsMessage.type) {
                            case DELETE:
                                clientsMessage.socketHandler.sendMessage(new OutputMessage("DELETED"));
                                break;
                            case SET:
                                clientsMessage.socketHandler.sendMessage(new OutputMessage("STORED"));
                                break;
                            default:
                        }
                        currentOperations.remove(operationNumber);
                        clientMessages.remove(operationNumber);
                    }
                    continue;
                case RECOVERY:
                    int lastOperation = curMessage.operationNumber;
                    StringBuilder msg = new StringBuilder();
                    msg.append("RECOVERY_RESPONSE ").append(view).append(" ");
                    for (int i = lastOperation + 1; i <= logger.getLastCommited(); i++) {
                        msg.append(logger.getOperation(i).toString()).append(" ");
                    }
                    curMessage.socketHandler.sendMessage(new OutputMessage(msg.toString()));
                    continue;
                default:
            }
        }
    }
}

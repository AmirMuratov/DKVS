package dkvs;

import dkvs.OperationLogger.OperationLogger;
import dkvs.kvsservice.KeyValueStorage;
import dkvs.sockets.SocketHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by amir.
 */
public class PrimaryServer implements Runnable {
    private int replicaNumber;
    private int view;
    private LinkedBlockingQueue<InputMessage> queue;
    private SocketHandler[] serverList;
    private int curOperationNumber;
    private KeyValueStorage service;
    private OperationLogger logger;
    private Map<Integer, Integer> currentOperations; //operationNumber -> number of received prepare_ok's
    private Map<Integer, InputMessage> clientMessages; //operationNumber -> message from client

    public PrimaryServer(int replicaNumber, LinkedBlockingQueue<InputMessage> queue,
                         SocketHandler[] serverList, int view) {
        this.currentOperations = new HashMap<>();
        this.clientMessages = new HashMap<>();
        this.serverList = serverList;
        this.replicaNumber = replicaNumber;
        this.queue = queue;
        this.service = new KeyValueStorage();
        curOperationNumber = 0;
        this.logger = new OperationLogger(replicaNumber, service);
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
                case SET:
                    key = curMessage.key;
                    value = curMessage.value;
                    curOperationNumber++;
                    logger.addSetOperation(key, value);
                    //if (serverList.size() < Configuration.getNumberOfServers() / 2) {
                    //    System.err.println("Can't reach consensus, servers working: " + serverList.size());
                    //}
                    for (SocketHandler handler : serverList) {
                        if (handler != null) {
                            String prepare = "PREPARE " + view +
                                    " " + curOperationNumber + " SET " + key + " " + value;
                            handler.sendMessage(new OutputMessage(prepare));
                        }
                    }
                    currentOperations.put(curOperationNumber, 1);
                    clientMessages.put(curOperationNumber, curMessage);
                    continue;
                case DELETE:
                    key = curMessage.key;
                    curOperationNumber++;
                    logger.addDeleteOperation(key);
                    //if (serverList.size() < Configuration.getNumberOfServers() / 2) {
                    //    System.err.println("Can't reach consensus, servers working: " + serverList.size());
                    //}
                    for (SocketHandler handler : serverList) {
                        if (handler != null) {
                            String prepare = "PREPARE " + view +
                                    " " + curOperationNumber + " DELETE " + key;
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
                default:
            }
        }
    }
}

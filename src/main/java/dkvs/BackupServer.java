package dkvs;

import dkvs.operationlogger.Operation;
import dkvs.operationlogger.OperationLogger;
import dkvs.kvsservice.KeyValueStorage;
import dkvs.sockets.ServerSocketListener;
import dkvs.sockets.SocketHandler;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by amir.
 */
public class BackupServer implements Runnable {

    private SocketHandler[] serverList;//serverNumber -> socket that handles connection to this server
    private int replicaNumber;
    private int view;
    private int opNumber;
    private OperationLogger logger;
    private BlockingQueue<InputMessage> queue;//main queue, where all messages come
    private KeyValueStorage service;
    private Queue<InputMessage> currentSetDeleteOp;
    private ServerSocketListener listener;


    public BackupServer(int replicaNumber, ServerSocketListener listener) {
        this.listener = listener;
        this.serverList = listener.getServerList();
        this.replicaNumber = replicaNumber;
        this.queue = listener.getInputQueue();
        this.service = new KeyValueStorage();
        //automatically loads log from file and commit operations to storage
        this.logger = new OperationLogger(replicaNumber, service);
        currentSetDeleteOp = new ArrayDeque<>();
        //?????
        view = 0;
    }

    private int getPrimary() {
        return view % Configuration.getNumberOfServers() + 1;
    }

    private void recover() {
        System.out.println("Starting recovery");
        for (SocketHandler handler : serverList) {
            if (handler != null) {
                handler.sendMessage(new OutputMessage("RECOVERY " + logger.getLastCommited()));
            }
        }
        List<Operation> log = null;
        int newView;
        int responses = 0;
        boolean recovered = false;
        while (true) {
            if (recovered) break;
            InputMessage curMessage;
            try {
                curMessage = queue.take();
            } catch (InterruptedException e) {
                return;
            }
            if (curMessage.type == MessageType.RECOVERY_RESPONSE) {
                responses++;
                newView = curMessage.viewNumber;
                if (curMessage.log != null) {
                    log = curMessage.log;
                }
                if (log != null && responses >= Configuration.getNumberOfServers() / 2 + 1) {
                    logger.commitOperations(log);
                    view = newView;
                    recovered = true;
                }
            }
        }
        System.out.println("Recovered");
    }

    private void viewChange(InputMessage lastMessage) {
        view++;
        System.out.println("Starting view change");
        for (SocketHandler handler : serverList) {
            if (handler != null) {
                handler.sendMessage(new OutputMessage("START_VIEW_CHANGE " + view + " " + replicaNumber));
            }
        }
        List<Operation> log = null;
        int startViewChanges = 1;
        int doViewChanges = 0;
        InputMessage curMessage = lastMessage;
        while (true) {
            if (curMessage.type.equals(MessageType.START_VIEW)) {
                view = curMessage.viewNumber;
                logger.clearLog();
                service = new KeyValueStorage();
                logger = new OperationLogger(replicaNumber, service);
                logger.commitOperations(curMessage.log);
                break;
            }
            switch (curMessage.type) {
                case DO_VIEW_CHANGE:
                    doViewChanges++;
                    if (log == null || log.size() < curMessage.log.size()) {
                        log = curMessage.log;
                    }
                    if (doViewChanges == Configuration.getNumberOfServers() / 2) {
                        view = curMessage.viewNumber;
                        logger.clearLog();
                        service = new KeyValueStorage();
                        logger = new OperationLogger(replicaNumber, service);
                        logger.commitOperations(log);
                        StringBuilder msg = new StringBuilder();
                        msg.append("START_VIEW ").append(view).append(" ");
                        for (int i = 0; i <= logger.getLastCommited(); i++) {
                            msg.append(logger.getOperation(i).toString()).append(" ");
                        }
                        for (SocketHandler handler : serverList) {
                            if (handler != null) {
                                handler.sendMessage(new OutputMessage(msg.toString()));
                            }
                        }
                        System.out.println("NEW PRIMARY: " + replicaNumber);
                        new PrimaryServer(replicaNumber, logger, listener, view).run();
                    }
                    break;
                case START_VIEW_CHANGE:
                    if (getPrimary() == replicaNumber) break;
                    view = curMessage.viewNumber;
                    startViewChanges++;
                    if (startViewChanges == Configuration.getNumberOfServers() / 2 + 1) {
                        StringBuilder msg = new StringBuilder();
                        msg.append("DO_VIEW_CHANGE ").append(view).append(" ");
                        for (int i = 0; i <= logger.getLastCommited(); i++) {
                            msg.append(logger.getOperation(i).toString()).append(" ");
                        }
                        serverList[getPrimary()].sendMessage(new OutputMessage(msg.toString()));
                    }
            }
            try {
                curMessage = queue.take();
            } catch (InterruptedException e) {
                return;
            }
        }
        System.out.println("Finishing view change");
    }


    @Override
    public void run() {
        if (logger.needRecovery()) {
            recover();
        }
        while (true) {
            InputMessage curMessage;
            try {
                curMessage = queue.take();
            } catch (InterruptedException e) {
                return;
            }
            switch (curMessage.type) {
                case DISCONNECT:
                    if (curMessage.replica == getPrimary())
                        viewChange(curMessage);
                    continue;
                case START_VIEW_CHANGE:
                case DO_VIEW_CHANGE:
                    if (curMessage.viewNumber > view)
                        viewChange(curMessage);
                    continue;
                case RECOVERY:
                    curMessage.socketHandler.sendMessage(new OutputMessage("RECOVERY_RESPONSE " + view + " " + replicaNumber));
                    continue;
                case COMMIT:
                    int opNumber = curMessage.operationNumber;
                    if (curMessage.socketHandler.getServerNum() == getPrimary()) {
                        logger.commitAllUntil(opNumber);
                    }
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
                    continue;
                case SET:
                    currentSetDeleteOp.add(curMessage);
                    serverList[getPrimary()].sendMessage(new OutputMessage("SET " + curMessage.op.getKey() + " " + curMessage.op.getValue()));
                    continue;
                case DELETE:
                    currentSetDeleteOp.add(curMessage);
                    serverList[getPrimary()].sendMessage(new OutputMessage("DELETE " + curMessage.op.getKey()));
                    continue;
                case STORED:
                case DELETED:
                case NOT_FOUND:
                    InputMessage clientMsg = currentSetDeleteOp.remove();
                    clientMsg.socketHandler.sendMessage(new OutputMessage(curMessage.type.toString()));
                    continue;
                default:
            }
        }
    }
}

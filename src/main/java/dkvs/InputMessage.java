package dkvs;

import dkvs.operationlogger.Operation;
import dkvs.sockets.SocketHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by amir.
 */
public class InputMessage {
    //STORED
    //NOT_FOUND
    //DELETED
    //GET key
    //SET key value
    //DELETE key
    //RECOVERY last_op
    //RECOVERY_RESPONSE view log

    //COMMIT viewNumber
    //PREPARE_OK viewNumber operation_number
    //PREPARE view_number operation_number (DELETE key | SET key value)

    //START_VIEW_CHANGE view replica
    //START_VIEW view log
    int replica;
    int viewNumber;
    int operationNumber;
    List<Operation> log;
    Operation op;
    String key;
    MessageType type;
    SocketHandler socketHandler;

    public InputMessage(String request, SocketHandler socket) {
        socketHandler = socket;
        String[] segments = request.split(" ");
        switch (segments[0]) {
            case "DISCONNECT":
                type = MessageType.DISCONNECT;
                replica = Integer.valueOf(segments[1]);
                return;
            case "STORED":
            case "NOT_FOUND":
            case "DELETED":
                type = MessageType.valueOf(segments[0]);
                return;
            case "GET":
                type = MessageType.GET;
                key = segments[1];
                return;
            case "RECOVERY":
                type = MessageType.RECOVERY;
                operationNumber = Integer.valueOf(segments[1]);
                return;
            case "SET":
                type = MessageType.SET;
                op = Operation.getSetOperation(segments[1], segments[2]);
                return;
            case "DELETE":
                type = MessageType.DELETE;
                op = Operation.getDeleteOperation(segments[1]);
                return;
            case "COMMIT":
                type = MessageType.COMMIT;
                operationNumber = Integer.valueOf(segments[1]);
                return;
            case "PREPARE_OK":
                type = MessageType.PREPARE_OK;
                operationNumber = Integer.valueOf(segments[1]);
                return;
            case "PREPARE":
                type = MessageType.PREPARE;
                viewNumber = Integer.valueOf(segments[1]);
                operationNumber = Integer.valueOf(segments[2]);
                //commitNumber = Integer.valueOf(segments[4]);
                switch (segments[3]) {
                    case "DELETE":
                        op = Operation.getDeleteOperation(segments[4]);
                        return;
                    case "SET":
                        op = Operation.getSetOperation(segments[4], segments[5]);
                        return;
                }
            case "RECOVERY_RESPONSE":
            case "START_VIEW":
            case "DO_VIEW_CHANGE":
                type = MessageType.valueOf(segments[0]);
                viewNumber = Integer.valueOf(segments[1]);
                if (segments.length == 3) {
                    replica = Integer.valueOf(segments[2]);
                    return;
                }
                int i = 2;
                log = new ArrayList<>();
                while (i < segments.length) {
                    if (segments[i].equals("SET")) {
                        log.add(Operation.getSetOperation(segments[i + 1], segments[i + 2]));
                        i += 3;
                    } else if (segments[i].equals("DELETE")) {
                        log.add(Operation.getDeleteOperation(segments[i + 1]));
                        i += 2;
                    }
                }
                return;
            case "START_VIEW_CHANGE":
                type = MessageType.START_VIEW_CHANGE;
                viewNumber = Integer.valueOf(segments[1]);
                replica = Integer.valueOf(segments[2]);
                return;
            default:
        }

    }


    @Override
    public String toString() {
        //TODO
        return null;
    }
}

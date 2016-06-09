package dkvs;

import dkvs.OperationLogger.Operation;
import dkvs.sockets.SocketHandler;

/**
 * Created by amir.
 */
public class InputMessage {
    //PING
    //PONG
    //COMMIT
    //GET key
    //SET key value
    //DELETE key
    //PREPARE_OK operation_number
    //PREPARE view_number operation_number (DELETE key | SET key value)
    int viewNumber;
    int operationNumber;
    //int commitNumber;
    Operation op;
    MessageType type;
    String key;
    String value;
    SocketHandler socketHandler;

    public InputMessage(String request, SocketHandler socket) {
        socketHandler = socket;
        System.out.println("REQUEST:|" + request + "|");
        String[] segments = request.split(" ");
        switch (segments[0]) {
            case "PING":
                type = MessageType.PING;
                return;
            case "PONG":
                type = MessageType.PONG;
                return;
            case "COMMIT":
                type = MessageType.COMMIT;
                return;
            case "GET":
                type = MessageType.GET;
                key = segments[1];
                return;
            case "SET":
                type = MessageType.SET;
                key = segments[1];
                value = segments[2];
                return;
            case "DELETE":
                type = MessageType.DELETE;
                key = segments[1];
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
            default:
        }

    }


    @Override
    public String toString() {

        //TODO
        return null;
    }
}

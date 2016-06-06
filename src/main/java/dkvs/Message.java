package dkvs;

import dkvs.sockets.SocketHandler;

/**
 * Created by amir.
 */
public class Message {
    //REQUEST (GET key | SET key value | DELETE key)
    //PING replicaNumber?
    //PONG replicaNumber?
    //PREPARE view_number operation_number (DELETE key | SET key value) commit_number
    //COMMIT
    //PREPARE_OK

    MessageType type;

    String key;
    String value;

    public Message(String request, SocketHandler socket) {
        String[] segments = request.split(" ");
        switch (segments[0]) {
            case "PING" :
                type = MessageType.PING;
                return;
            case "PONG" :
                type = MessageType.PONG;
                return;
            case "COMMIT" :
                type = MessageType.COMMIT;
                return;
            case "REQUEST" :
                switch (segments[1]) {
                    case "GET" :
                    case "SET" :
                    case "DELETE" :

                }
            case "PREPARE_OK" :
            case "PREPARE" :
        }
    }



    @Override
    public String toString() {

        //TODO
        return null;
    }
}

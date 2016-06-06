package dkvs.sockets;

import dkvs.Configuration;
import dkvs.Message;

import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by amir.
 */
public class Connector {
    private int replicaNumber;
    private Map<Integer, SocketHandler> serverList;
    private LinkedBlockingQueue<Message> inputQueue;
    public Connector(int replicaNumber, LinkedBlockingQueue<Message> inputQueue, Map<Integer,SocketHandler> serverList) {
        this.replicaNumber = replicaNumber;
        this.serverList = serverList;
        this.inputQueue = inputQueue;
    }

    public void connect() {
        for (int i = 0; i < Configuration.getNumberOfservers(); i++) {
            connectToServer(i);
        }
    }

    public void connectToServer(int serverNumber) {
        //Trying to connect to only
        //to (replicaNumber + 1) % size, (replicaNumber + 2) % size ... (replicaNumber + size / 2) % size
        int num = serverNumber >= replicaNumber ? serverNumber : serverNumber + Configuration.getNumberOfservers();
        if (num - replicaNumber > Configuration.getNumberOfservers() / 2) {
            return;
        }
        new Thread(() -> {
            int port = Configuration.getPort(serverNumber);
            String address = Configuration.getIP(serverNumber);
            try {
                Socket socket = new Socket(address, port);
                serverList.put(serverNumber, new SocketHandler(socket, inputQueue));
            } catch (IOException e) {
                System.err.println("Can't connect to server number " + serverNumber);
            }
        }).start();
    }

}

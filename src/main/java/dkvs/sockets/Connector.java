package dkvs.sockets;

import dkvs.Configuration;
import dkvs.InputMessage;
import dkvs.OutputMessage;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by amir.
 */
public class Connector {
    private int replicaNumber;
    private SocketHandler[] serverList;
    private LinkedBlockingQueue<InputMessage> inputQueue;
    public Connector(int replicaNumber, LinkedBlockingQueue<InputMessage> inputQueue, SocketHandler[] serverList) {
        this.replicaNumber = replicaNumber;
        this.serverList = serverList;
        this.inputQueue = inputQueue;
    }

    public void connect() {
        for (int i = 1; i <= Configuration.getNumberOfServers(); i++) {
            connectToServer(i);
        }
    }

    public void connectToServer(int serverNumber) {
        //Trying to connect to only
        //to (replicaNumber + 1) % size, (replicaNumber + 2) % size ... (replicaNumber + size / 2) % size
        int num = serverNumber > replicaNumber ? serverNumber : serverNumber + Configuration.getNumberOfServers();
        if (num - replicaNumber > Configuration.getNumberOfServers() / 2) {
            return;
        }
        assert 0 < serverNumber && serverNumber <= Configuration.getNumberOfServers();
        new Thread(() -> {
            int port = Configuration.getPort(serverNumber);
            String address = Configuration.getIP(serverNumber);
            try {
                Socket socket = new Socket(address, port);
                if (serverList[serverNumber] != null) serverList[serverNumber].stop();
                serverList[serverNumber] = new SocketHandler(socket, inputQueue, serverNumber, serverList);
                serverList[serverNumber].sendMessage(new OutputMessage("node " + replicaNumber));
            } catch (IOException e) {
                serverList[serverNumber] = null;
                System.err.println("Can't connect to server number " + serverNumber);
            }
        }).start();
    }

}

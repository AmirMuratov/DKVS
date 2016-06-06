package dkvs.sockets;

import dkvs.Configuration;
import dkvs.Message;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

/**
 * Created by amir.
 */
public class ServerSocketListener {
    private ServerSocket serverSocket;
    private Queue<Message> inputQueue;
    private Set<SocketHandler> clients = new HashSet<>();//TODO
    private Map<Integer, SocketHandler> serverList;

    public ServerSocketListener(int port, Queue<Message> inputQueue, Map<Integer, SocketHandler> serverList) {
        this.serverList = serverList;
        this.inputQueue = inputQueue;
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            System.err.println("Can't create server socket on given port");
        }
    }

    public void acceptConnection() {
        try {
            Socket curSocket = serverSocket.accept();
            SocketHandler handler = new SocketHandler(curSocket, inputQueue);
            int serverNumber =  Configuration.getServerNumber(curSocket.getRemoteSocketAddress().toString());
            if (serverNumber != -1) {
                serverList.put(serverNumber, handler);
            } else {
                clients.add(handler);
            }
        } catch (IOException e) {
            System.err.println("Error while accepting");
        }
    }

    public void listen() {
        new Thread(() -> {
            while (true) {
                acceptConnection();
            }
        }).start();
    }

    public void stop() {
        //TODO
    }
}

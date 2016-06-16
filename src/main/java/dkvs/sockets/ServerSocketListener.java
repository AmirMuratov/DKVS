package dkvs.sockets;

import dkvs.Configuration;
import dkvs.InputMessage;
import dkvs.OutputMessage;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by amir.
 */
public class ServerSocketListener {
    private ServerSocket serverSocket;
    BlockingQueue<InputMessage> inputQueue;
    Set<SocketHandler> clientsList;
    SocketHandler[] serverList;
    int replicaNumber;
    private Timer timer;

    public BlockingQueue<InputMessage> getInputQueue() {
        return inputQueue;
    }

    public SocketHandler[] getServerList() {
        return serverList;
    }

    public ServerSocketListener(int port, int replicaNumber) {
        clientsList = new HashSet<>();
        this.serverList = new SocketHandler[Configuration.getNumberOfServers() + 1];
        this.inputQueue = new LinkedBlockingQueue<>();
        this.replicaNumber = replicaNumber;
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            System.err.println("Can't create server socket on given port");
        }
    }

    public void start() {
        startTimer();
        new Thread(() -> {
            while (true) {
                try {
                    Socket curSocket = serverSocket.accept();
                    SocketHandler handler = new SocketHandler(curSocket, -1, this);
                    clientsList.add(handler);
                } catch (IOException e) {
                    System.out.println("stopping listening server socket");
                    break;
                }
            }
        }).start();
        try {
            Thread.sleep(Configuration.getTimeout() * 2);
        } catch (InterruptedException ignored) {
        }
    }

    public void stop() {
        clientsList.forEach(SocketHandler::stop);
        Arrays.asList(serverList).forEach(SocketHandler::stop);
        timer.cancel();
        try {
            serverSocket.close();
        } catch (IOException e) {
            System.err.println("Can't close serverSocket((");
        }
    }

    public void connect() {
        for (int i = 1; i <= Configuration.getNumberOfServers(); i++) {
            connectToServer(i);
        }
    }

    private void connectToServer(int serverNumber) {
        //Trying to connect only
        //to (replicaNumber + 1) % size, (replicaNumber + 2) % size ... (replicaNumber + size / 2) % size
        int num = serverNumber > replicaNumber ? serverNumber : serverNumber + Configuration.getNumberOfServers();
        if (num - replicaNumber > Configuration.getNumberOfServers() / 2) {
            return;
        }
        assert 0 < serverNumber && serverNumber <= Configuration.getNumberOfServers();
        new Thread(() -> {
            int port = Configuration.getPort(serverNumber);
            String address = Configuration.getIP(serverNumber);
            if (serverList[serverNumber] != null) serverList[serverNumber].stop();
            try {
                Socket socket = new Socket(address, port);
                serverList[serverNumber] = new SocketHandler(socket, serverNumber, this);
                serverList[serverNumber].sendMessage(new OutputMessage("node " + replicaNumber));
            } catch (IOException e) {
                serverList[serverNumber] = null;
                System.err.println("Can't connect to server number " + serverNumber);
            }
        }).start();
    }

    private void startTimer() {
        timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= Configuration.getNumberOfServers(); i++) {
                    sb.append(i == replicaNumber ? "O" : serverList[i] == null ? "-" : "+").append(" ");
                }
                System.out.println(sb);
                for (int i = 1; i <= Configuration.getNumberOfServers(); i++) {
                    if (serverList[i] != null) {
                        if (System.currentTimeMillis() - serverList[i].getLastResponse() > Configuration.getTimeout()) {
                            serverList[i].sendMessage(new OutputMessage("PING"));
                        }
                        if (System.currentTimeMillis() - serverList[i].getLastResponse() > 2 * Configuration.getTimeout()) {
                            System.out.println("Breaking connection with " + i);
                            serverList[i].stop();
                            serverList[i] = null;
                            inputQueue.add(new InputMessage("DISCONNECT " + i, null));
                        }
                    }
                    if (serverList[i] == null) {
                        connectToServer(i);
                    }
                }
                for (Iterator<SocketHandler> it = clientsList.iterator(); it.hasNext(); ) {
                    SocketHandler handler = it.next();
                    if (System.currentTimeMillis() - handler.getLastResponse() > 5 * Configuration.getTimeout()) {
                        handler.stop();
                        it.remove();
                    }
                }
            }
        }, 0, Configuration.getTimeout());
    }
}

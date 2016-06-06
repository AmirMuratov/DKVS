package dkvs;

import dkvs.sockets.Connector;
import dkvs.sockets.ServerSocketListener;
import dkvs.sockets.SocketHandler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class Main {

    public static void main(String[] args) {
        int replicaNumber = 1;
        if (args != null && args.length > 0) {
            replicaNumber = Integer.valueOf(args[0]);
        }
        Map<Integer, SocketHandler> serverList = new ConcurrentHashMap<>();
        LinkedBlockingQueue<Message> inputQueue = new LinkedBlockingQueue<>();
        ServerSocketListener listener = new ServerSocketListener(Configuration.getPort(replicaNumber), inputQueue, serverList);
        listener.listen();
        Connector connector = new Connector(replicaNumber, inputQueue, serverList);
        connector.connect();
        BackupServer server = new BackupServer(replicaNumber, inputQueue, serverList);
        new Thread(server).start();
    }
}

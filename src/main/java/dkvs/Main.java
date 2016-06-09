package dkvs;

import dkvs.sockets.Connector;
import dkvs.sockets.ServerSocketListener;
import dkvs.sockets.SocketHandler;

import java.util.concurrent.LinkedBlockingQueue;

public class Main {

    public static void main(String[] args) {
        int replicaNumber = 1;
        if (args != null && args.length > 0) {
            replicaNumber = Integer.valueOf(args[0]);
        }
        SocketHandler[] serverList = new SocketHandler[Configuration.getNumberOfServers() + 1];//KOSTYL
        LinkedBlockingQueue<InputMessage> inputQueue = new LinkedBlockingQueue<>();
        ServerSocketListener listener = new ServerSocketListener(Configuration.getPort(replicaNumber), inputQueue, serverList);
        listener.listen();
        Connector connector = new Connector(replicaNumber, inputQueue, serverList);
        connector.connect();
        HeartBeat heartBeat = new HeartBeat(serverList, connector);
        heartBeat.start();
        //testing
        if (replicaNumber != 1) {
            BackupServer server = new BackupServer(replicaNumber, inputQueue, serverList);
            new Thread(server).start();
        } else {
            PrimaryServer server = new PrimaryServer(replicaNumber, inputQueue, serverList, 0);
            new Thread(server).start();
        }
    }
}

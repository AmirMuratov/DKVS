package dkvs;

import dkvs.sockets.Connector;
import dkvs.sockets.SocketHandler;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by amir.
 */
public class HeartBeat {
    Timer timer;
    SocketHandler[] serverList;
    Connector connector;
    public HeartBeat(SocketHandler[] serverList, Connector connector)  {
        this.serverList = serverList;
        this.connector = connector;
        timer = new Timer();
    }

    public void start() {
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= Configuration.getNumberOfServers(); i++) {
                    sb.append(serverList[i] == null ? "0" : "1").append(" ");
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
                        }
                    }
                    if (serverList[i] == null) {
                        connector.connectToServer(i);
                    }
                }
            }
        }, Configuration.getTimeout(), Configuration.getTimeout());

    }
    public void stop() {
        timer.cancel();
    }
}

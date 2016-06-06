package dkvs.OperationLogger;

import dkvs.kvsservice.KeyValueStorage;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by amir.
 */
public class OperationLogger {
    private List<Operation> log;
    private Writer writer;
    private int lastToCommit;
    private static final String FILENAME = "logs/dksv_%d.log";
    private KeyValueStorage service;

    public OperationLogger(int id, KeyValueStorage service) {
        this.service = service;
        lastToCommit = 0;
        log = new ArrayList<>();
        File logFile = new File(String.format(FILENAME, id));
        if (logFile.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
                String line = reader.readLine();
                while (line != null) {
                    String[] parts = line.split(" ");
                    Operation op = parts[0].equals("DELETE") ? Operation.getDeleteOperation(parts[1])
                            : Operation.getSetOperation(parts[1], parts[2]);
                    log.add(op);
                    line = reader.readLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            writer = new BufferedWriter(new FileWriter(logFile, true));
        } catch (IOException e) {
            System.err.println("Logger can't write to file");
        }
        commitAll();
    }

    public void addDeleteOperation(String key) {
        Operation op = Operation.getDeleteOperation(key);
        log.add(op);
    }

    public void addSetOperation(String key, String value) {
        Operation op = Operation.getSetOperation(key, value);
        log.add(op);
    }

    private void flushOp(Operation op) {
        try {
            writer.write(op.toString());
            writer.flush();
        } catch (IOException e) {
            System.err.println("Can't write to log file");
        }
    }

    public void commitAll() {
        for (; lastToCommit < log.size(); lastToCommit++) {
            Operation curOp = log.get(lastToCommit);
            curOp.setCommited();
            service.commit(curOp); //commit operation to map
            flushOp(curOp); //write log
        }
    }

}

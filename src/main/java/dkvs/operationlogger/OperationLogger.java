package dkvs.operationlogger;

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
    private int lastCommited;
    private static final String FILENAME = "logs/dksv_%d.log";
    private KeyValueStorage service;
    private boolean recovery;
    File logFile;

    public Operation getOperation(int id) {
        return log.get(id);
    }
    public int getLastCommited() {
        return lastCommited;
    }
    public int getLastPrepared() {
        return log.size() - 1;
    }
    public boolean needRecovery() {
        return recovery;
    }
    public KeyValueStorage getService() {
        return service;
    }

    public OperationLogger(int id, KeyValueStorage service) {
        this.service = service;
        lastCommited = -1;
        log = new ArrayList<>();
        recovery = false;
        logFile = new File(String.format(FILENAME, id));
        if (logFile.exists()) {
            recovery = true;
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
        while (lastCommited + 1 < log.size()) {
            lastCommited++;
            Operation curOp = log.get(lastCommited);
            curOp.setCommited();
            service.commit(curOp); //commit operation to map
        }
    }
    public void clearLog() {
        logFile.delete();
    }

    public void addDeleteOperation(String key) {
        Operation op = Operation.getDeleteOperation(key);
        log.add(op);
    }

    public void addSetOperation(String key, String value) {
        Operation op = Operation.getSetOperation(key, value);
        log.add(op);
    }

    public void addOperation(Operation op) {
        log.add(op);
    }

    private void flushOp(Operation op) {
        try {
            writer.write(op.toString() + "\n");
            writer.flush();
        } catch (IOException e) {
            System.err.println("Can't write to log file");
        }
    }

    public void commitAll() {
        while (lastCommited + 1 < log.size()) {
            lastCommited++;
            Operation curOp = log.get(lastCommited);
            curOp.setCommited();
            service.commit(curOp); //commit operation to map
            flushOp(curOp); //write log
        }
    }

    public void commitOperations(List<Operation> operations) {
        operations.forEach(log::add);
        commitAll();
    }

    public void commitAllUntil(int threshold) {
        while (lastCommited + 1 < threshold) {
            lastCommited++;
            Operation curOp = log.get(lastCommited);
            curOp.setCommited();
            service.commit(curOp); //commit operation to map
            flushOp(curOp); //write log
        }
    }

}

package dkvs.kvsservice;

import dkvs.operationlogger.Operation;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by amir.
 */
public class KeyValueStorage {
    private Map<String, String> storage;

    public KeyValueStorage() {
        storage = new HashMap<>();
    }

    public String get(String key) {
        return storage.get(key);
    }
    public void commit(Operation op) {
        if (op.getType().equals(Operation.Type.DELETE)) {
            storage.remove(op.getKey());
        }
        if (op.getType().equals(Operation.Type.SET)) {
            storage.put(op.getKey(), op.getValue());
        }
    }
}

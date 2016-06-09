package dkvs.OperationLogger;

import java.util.Map;

/**
 * Created by amir.
 */
public class Operation {

    public enum Type {
        DELETE, SET
    }

    private boolean isCommited;
    private Type type;
    private String key;
    private String value;

    public static Operation getDeleteOperation(String key) {
        Operation op = new Operation();
        op.isCommited = false;
        op.type = Type.DELETE;
        op.key = key;
        return op;
    }
    public static Operation getSetOperation(String key, String value) {
        Operation op = new Operation();
        op.isCommited = false;
        op.type = Type.SET;
        op.key = key;
        op.value = value;
        return op;
    }

    private Operation() {
    }

    public Type getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public void setCommited() {
        isCommited = true;
    }

    public boolean isCommited() {
        return isCommited;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append(type.toString()).append(" ").append(key);
        if (type.equals(Type.SET)) {
            result.append(" ").append(value);
        }
        result.append("\n");
        return result.toString();
    }

}

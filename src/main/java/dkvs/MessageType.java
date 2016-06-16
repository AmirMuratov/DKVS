package dkvs;

/**
 * Created by amir.
 */
public enum MessageType {
    GET, SET, DELETE,
    COMMIT, PREPARE_OK, PREPARE,
    STORED, NOT_FOUND, DELETED,
    DISCONNECT,
    START_VIEW_CHANGE, DO_VIEW_CHANGE, START_VIEW,
    RECOVERY, RECOVERY_RESPONSE
}
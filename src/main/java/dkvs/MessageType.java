package dkvs;

/**
 * Created by amir.
 */
public enum MessageType {
    REQUEST, PING, PONG, COMMIT, PREPARE_OK, PREPARE,
    START_VIEW_CHANGE, DO_VIEW_CHANGE
}
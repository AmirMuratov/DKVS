package dkvs;

/**
 * Created by amir.
 */
public class OutputMessage {
    MessageType type;
    private String text;
    public OutputMessage(String text) {
        this.text = text;
    }
    //GET, SET, DELETE, COMMIT, PREPARE_OK, PREPARE,
    //START_VIEW_CHANGE, DO_VIEW_CHANGE, STORED, NOT_FOUND, DELETED
    //public OutputMessage getMessage() {
    //    OutputMessage msg = new OutputMessage();
    //    msg.type = ;
    //    return msg;
    //}

    public String getText() {

        return text;
    }
}

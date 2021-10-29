package com.doodle;

/**
 * Class to read required attributes from input kafka stream
 */
public class InboundMessage {
    String uid;
    Long ts;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }
}

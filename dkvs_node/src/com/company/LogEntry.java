package com.company;

/**
 * Created by belonogov on 5/23/16.
 */
public class LogEntry {
    private String key;
    private String value;
    private int termId;

    LogEntry(String data) {
        String[] tmp = data.split("|");
        assert(tmp.length == 3);
        key = tmp[0];
        value = tmp[1];
        termId = Integer.valueOf(tmp[2]);
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public int getTermId() {
        return termId;
    }
}

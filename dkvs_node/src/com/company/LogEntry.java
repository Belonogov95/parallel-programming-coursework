package com.company;

import sun.rmi.runtime.Log;

/**
 * Created by belonogov on 5/23/16.
 */
public class LogEntry {
    private String key;
    private String value;
    private int termId;

    public static LogEntry myDeserialization (String data) {
        String[] tmp = data.split("|");
        assert(tmp.length == 3);
        LogEntry answer = new LogEntry();
        answer.key = tmp[0];
        answer.value = tmp[1];
        answer.termId = Integer.valueOf(tmp[2]);
        return answer;
    }

    public static String mySerialization(LogEntry entry) {
        return entry.key + "|" + entry.value + "|" + entry.termId;
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

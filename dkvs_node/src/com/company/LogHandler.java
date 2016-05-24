package com.company;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by belonogov on 5/24/16.
 */
public class LogHandler {
    private ArrayList< LogEntry > dataLog;
    private BufferedWriter bf;
    private String logFile;
    private String termFile;
    private int term;

    private int lastRecord;


    public LogHandler(int myId) {
        logFile = "dkvs_" + myId + ".log";
        termFile = "dkvs_" + myId + ".term";
        dataLog = new ArrayList<>();
        readLog();
        readTerm();
    }

    public ArrayList<LogEntry> getDataLog() {
        return dataLog;
    }

    private void readLog() {
        dataLog.clear();
        try (BufferedReader br = new BufferedReader(new FileReader(logFile))) {
            String s;
            while ((s = br.readLine()) != null)
                dataLog.add(LogEntry.myDeserialization(s));
            lastRecord = dataLog.size();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void reWriteLog() {
        try (BufferedWriter bf = new BufferedWriter(new FileWriter(logFile))){
            for (LogEntry aDataLog : dataLog)
                bf.write(LogEntry.mySerialization(aDataLog) + System.lineSeparator());
            lastRecord = dataLog.size();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void appendLog() {
        try (BufferedWriter bf = new BufferedWriter(new FileWriter(logFile, true))) {
            for (; lastRecord < dataLog.size(); lastRecord++) {
                bf.write(LogEntry.mySerialization(dataLog.get(lastRecord)) + System.lineSeparator());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void reWriteTerm(int nTerm) {
        term = nTerm;
        try (BufferedWriter bf = new BufferedWriter(new FileWriter(termFile))) {
            bf.write(term + System.lineSeparator());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readTerm() {
        try (BufferedReader bf = new BufferedReader(new FileReader(termFile))) {
            term = Integer.valueOf(bf.readLine());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



}

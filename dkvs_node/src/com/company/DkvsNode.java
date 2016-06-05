package com.company;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Created by belonogov on 22.05.16.
 */

public class DkvsNode implements Runnable {
    @Override
    public void run() {
        try {
            init();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public enum Status {
        FOLLOWER, CANDIDATE, LEADER;
    }

    ;

    private final String ACCEPTED = "ACCEPTED";
    private final String SET = "set";
    private final String GET = "get";
    private final String DELETE = "delete";
    private final String STORED = "STORED";
    private final String DELETED = "DELETED";
    private final String NOT_FOUND = "NOT_FOUND";
    private final String NODE = "node";
    private final String VALUE = "VALUE";
    private final String TO_LEADER = "TO_LEADER";
    private final String REQUEST_VOTE = "REQUEST_VOTE";
    private final String RESPONSE_VOTE = "RESPONSE_VOTE";
    private final String TRUE = "TRUE";
    private final String FALSE = "FALSE";
    private final String APPEND_ENTRIES = "APPEND_ENTRIES";
    private final String APPEND_ENTRIES_RESPONSE = "APPEND_ENTRIES_RESPONSE";


    private Selector selector;
    private int nodeId;
    private int myPort;
    private int timeL, timeR, heartBeat;
    private int timeout;

    private int leaderId;
    private long lastLeaderPing;
    private int curTermId;
    private Status status;
    private long lastConnect;

    // candidate data
    private int voteForMe;
    private boolean termVote;
    private int cntNodes;

    // leader data
    private long lastHeardBeat;
    private int commitIndex;

    private Map<String, String> dataStorage; /// main map
    private Map<Integer, SelectionKey> friendsKeys;
    private Map<Integer, Integer> ports;
    private Map<Integer, Integer> matchIndex;
    private Map<Integer, Integer> nextIndex;

    private LogHandler logHandler;

    public DkvsNode(int nodeId, int timeL, int timeR, int heartBeat, Map<Integer, Integer> allPorts) throws IOException {
        assert (timeL < timeR);
        assert (heartBeat * 2 <= timeL);
        this.nodeId = nodeId;
        this.myPort = allPorts.get(nodeId);
        this.timeL = timeL;
        this.timeR = timeR;
        this.heartBeat = heartBeat;
        this.ports = allPorts;
        this.termVote = false; // maybe I should to write it to file
        this.cntNodes = allPorts.size();
        this.matchIndex = new TreeMap<>();
        this.nextIndex = new TreeMap<>();
        this.lastConnect = 0;
        this.lastLeaderPing = 0;

        ports.remove(nodeId);
        dataStorage = new TreeMap<>();
        commitIndex = 0;
        leaderId = -1;

        friendsKeys = new TreeMap<>();
    }


    void writeMessage(SelectionKey key, String message) throws ClosedChannelException {
        message = message + System.lineSeparator();
        ChannelHelper helper = (ChannelHelper) key.attachment();
        helper.getBuffer().put(message.getBytes(Charset.forName("UTF-8")));
        SocketChannel sc = (SocketChannel) key.channel();
        assert(key.isValid());
        sc.register(selector, key.interestOps() | SelectionKey.OP_WRITE, key.attachment());
    }

    private void resetToFollower(int newTermId) {
        logHandler.reWriteTerm(newTermId);
        status = Status.FOLLOWER;
        termVote = false;
        voteForMe = 0;
    }

    private void sendHeardBeat() throws ClosedChannelException {
        for (Map.Entry<Integer, SelectionKey> entry : friendsKeys.entrySet()) {
            StringBuilder message = new StringBuilder();
            int followerId = entry.getKey();

            message.append(APPEND_ENTRIES + " ");
            message.append(leaderId + " ");
            int nIndex = nextIndex.get(followerId);
            message.append(nextIndex.get(nIndex) + " ");
            message.append(logHandler.getTermById(nextIndex.get(nIndex)) + " ");
            message.append(commitIndex + " ");
            message.append("!");
            for (int i = nIndex + 1; i < logHandler.dataLog.size(); i++) {
                message.append(LogEntry.mySerialization(logHandler.dataLog.get(i)));
                if (i + 1 < logHandler.dataLog.size())
                    message.append(",");
            }
            writeMessage(entry.getValue(), message.toString());
        }
    }

    private void commitEntries(int leaderCommit) {
        if (commitIndex < leaderCommit) {
            for (int i = commitIndex; i < Math.min(logHandler.dataLog.size(), leaderCommit); i++) {
                if (logHandler.dataLog.get(i).getOp().equals(DELETE)) {
                    dataStorage.remove(logHandler.dataLog.get(i).getKey());
                } else {
                    assert (logHandler.dataLog.get(i).getOp().equals(SET));
                    dataStorage.put(logHandler.dataLog.get(i).getKey(), logHandler.dataLog.get(i).getValue());
                }
            }
            commitIndex = leaderCommit;
        }
    }


    private void handleSocket(SelectionKey key) throws ClosedChannelException {
        ChannelHelper helper = (ChannelHelper) key.attachment();
        StringBuilder buffer = helper.getsBuilder();
        int pos = buffer.indexOf(System.lineSeparator());
        if (pos == -1) return;

        System.err.println("buffer: " + buffer.toString());
        //System.err.println("pos: " + pos);
        String query = buffer.substring(0, pos);
        //System.err.println("Query " + query);
        int space = query.indexOf(" ");

        System.err.println(query);

        assert (space != -1);

        String command = query.substring(0, space);
        String args = query.substring(space).trim();
        if (command.equals(NODE)) {
            assert (helper.getNodeId() == -1);
            int id = Integer.valueOf(args.trim());
            helper.setNodeId(id);
            SelectionKey tmpKey = friendsKeys.get(id);
            if (tmpKey != null && ((SocketChannel) tmpKey.channel()).isConnected()) {
                try {
                    ((SocketChannel) tmpKey.channel()).close();
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(0);
                }
                tmpKey.cancel();
            }

            friendsKeys.put(id, key);
            //writeMessage(key, ACCEPTED);
        }
        if (command.equals(GET)) {
            assert (helper.getNodeId() == -1);
            String res = dataStorage.get(args);
            if (res == null)
                writeMessage(key, NOT_FOUND);
            else
                writeMessage(key, VALUE + " " + query + " " + res);
        }
        if (command.equals(SET)) {
            assert (helper.getNodeId() == -1);
            if (leaderId != -1) {
                writeMessage(key, STORED);
                writeMessage(friendsKeys.get(leaderId), TO_LEADER + " " + command);
            }
        }
        if (command.equals(DELETE)) {
            assert (helper.getNodeId() == -1);
            String res = dataStorage.get(args);
            if (leaderId != -1) {
                if (res == null)
                    writeMessage(key, NOT_FOUND);
                else
                    writeMessage(key, DELETED);
                writeMessage(friendsKeys.get(leaderId), TO_LEADER + " " + command);
            }
        }

        if (command.equals(TO_LEADER)) {
            if (leaderId == nodeId) {
                int nextSpace = args.indexOf(" ");
                command = args.substring(0, nextSpace);
                args = args.substring(nextSpace).trim();
                if (command.equals(DELETE)) {
                    logHandler.dataLog.add(new LogEntry(command, args, "", curTermId));
                } else if (command.equals(SET)) {
                    String[] setArgs = args.split(" ");
                    assert (setArgs.length == 2);
                    logHandler.dataLog.add(new LogEntry(command, setArgs[0], setArgs[1], curTermId));
                } else
                    assert (false);
            }
        }


        if (command.equals(REQUEST_VOTE)) {
            String[] tmp = args.split(" ");
            assert (tmp.length == 4);
            int term = Integer.valueOf(tmp[0]);
            int candidateId = Integer.valueOf(tmp[1]);
            int lastLogIndex = Integer.valueOf(tmp[2]);
            int lastLogTerm = Integer.valueOf(tmp[3]);

            if (logHandler.getTerm() < term) {
                resetToFollower(term);
                termVote = true;
                writeMessage(friendsKeys.get(candidateId), RESPONSE_VOTE + " " + logHandler.getTerm() + " " + TRUE);
            } else if (logHandler.getTerm() > term || (logHandler.getTerm() == term && termVote)) {
                writeMessage(friendsKeys.get(candidateId), RESPONSE_VOTE + " " + logHandler.getTerm() + " " + FALSE);
            } else {
                assert (logHandler.getTerm() == term);
                assert (!termVote);
                int myLastLogTerm = logHandler.getLastTerm();
                int myLastLogIndex = logHandler.getLastIndex();
                if (myLastLogTerm < lastLogTerm || (myLastLogTerm == lastLogTerm && myLastLogIndex <= lastLogIndex)) {
                    termVote = true;
                    writeMessage(friendsKeys.get(candidateId), RESPONSE_VOTE + " " + logHandler.getTerm() + " " + TRUE);
                } else {
                    writeMessage(friendsKeys.get(candidateId), RESPONSE_VOTE + " " + logHandler.getTerm() + " " + FALSE);
                }
            }
        }

        if (command.equals(RESPONSE_VOTE)) {
            String tmp[] = args.split(" ");
            assert (tmp.length == 2);
            int term = Integer.valueOf(tmp[0]);
            boolean flag = Boolean.valueOf(tmp[1]);
            if (term > logHandler.getTerm()) {
                resetToFollower(term);
            }
            if (flag) {
                voteForMe++;
                if (voteForMe * 2 > cntNodes) {
                    status = Status.LEADER;
                    matchIndex.clear();
                    nextIndex.clear();
                    for (Integer id : friendsKeys.keySet()) {
                        nextIndex.put(id, logHandler.getLastIndex());
                        matchIndex.put(id, 0);
                    }
                    commitIndex = 0;

                    sendHeardBeat();

                }
            }
        }

        if (command.equals(APPEND_ENTRIES)) {
            lastHeardBeat = System.currentTimeMillis() - stTime; /// TODO
            String tmp[] = args.split("!");
            assert (tmp.length == 2);
            String g[] = tmp[0].trim().split(" ");
            assert (g.length == 5);
            String newEntries[] = tmp[1].trim().split(",");
            int term = Integer.valueOf(g[0]);
            int leaderId = Integer.valueOf(g[1]);
            int prevLogIndex = Integer.valueOf(g[2]);
            int prevLogTerm = Integer.valueOf(g[3]);
            int leaderCommit = Integer.valueOf(g[4]);

            if (logHandler.getTerm() > term) {
                writeMessage(friendsKeys.get(leaderId), APPEND_ENTRIES_RESPONSE + " " + logHandler.getTerm() + " " + FALSE + " " + (-1));
            } else if (prevLogIndex == -1 || (prevLogIndex < logHandler.dataLog.size() && logHandler.dataLog.get(prevLogIndex).getTermId() == prevLogTerm)) {
                for (; logHandler.dataLog.size() > (prevLogIndex + 1); ) {
                    logHandler.dataLog.remove(logHandler.dataLog.size() - 1);
                }
                for (int i = 0; i < newEntries.length; i++)
                    logHandler.dataLog.add(LogEntry.myDeserialization(newEntries[i]));

                writeMessage(friendsKeys.get(leaderId), APPEND_ENTRIES_RESPONSE + " " + term + " " + TRUE + " " + logHandler.dataLog.size());

                commitEntries(leaderCommit);

            } else {
                writeMessage(friendsKeys.get(leaderId), APPEND_ENTRIES_RESPONSE + " " + logHandler.getTerm() + " " + FALSE + " " + (-1));
            }

        }

        if (command.equals(APPEND_ENTRIES_RESPONSE)) {
            String tmp[] = args.split(" ");
            assert (tmp.length == 3);
            int term = Integer.valueOf(tmp[0]);
            boolean flag = Boolean.valueOf(tmp[1]);
            int mIndex = Integer.valueOf(tmp[2]);
            int id = ((ChannelHelper) key.attachment()).getNodeId();
            assert (term <= logHandler.getTerm());
            if (flag) {
                matchIndex.put(id, mIndex);
                nextIndex.put(id, logHandler.dataLog.size());
                ArrayList<Integer> mLen = new ArrayList<>();
                mLen.add(logHandler.dataLog.size());
                for (Integer len : matchIndex.values()) {
                    mLen.add(len);
                }
                assert (mLen.size() == cntNodes);
                int p = cntNodes / 2;
                int newCommitIndex = mLen.get(p);
                assert (commitIndex <= newCommitIndex);
                commitEntries(newCommitIndex);


            } else {
                assert (nextIndex.get(id) >= 0);
                nextIndex.put(id, nextIndex.get(id) - 1);
            }
        }

        helper.setsBuilder(new StringBuilder(buffer.substring(pos + System.lineSeparator().length(), buffer.length())));
        handleSocket(key);
    }


    void connectToNode(int nodeId) throws IOException {
        SocketChannel sc = SocketChannel.open();
        sc.configureBlocking(false);
        sc.connect(new InetSocketAddress(ports.get(nodeId)));
        SelectionKey key = sc.register(selector, SelectionKey.OP_CONNECT);
        friendsKeys.put(nodeId, key);
        key.attach(new ChannelHelper(nodeId)); // TODO
    }

    void init() throws IOException {
        selector = Selector.open();
        logHandler = new LogHandler(nodeId);
        curTermId = logHandler.getTerm();

        new Thread(new TestWakeUp(selector, 500)).start();

        //System.err.println(Thread.currentThread().getName());
        Random random = new Random();
        timeout = timeL + random.nextInt(timeR - timeL);

        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.configureBlocking(false);
        ServerSocket ss = ssc.socket();
        InetSocketAddress address = new InetSocketAddress(myPort);
        ss.bind(address);
        ssc.register(selector, SelectionKey.OP_ACCEPT);

        //for (Map.Entry<Integer, Integer> entry : ports.entrySet()) {
        //connectToNode(entry.getKey());
        //}
        status = Status.FOLLOWER;
        go();
    }


    private long stTime;

    boolean isReady(SelectionKey key) {
        if (key == null) return false;
        SocketChannel socketChannel = (SocketChannel) key.channel();
        if (socketChannel.socket().isClosed()) return false;
        if (!socketChannel.socket().isConnected()) return false;
        return true;
    }

    void go() throws IOException {

        System.err.println("nodeId tmr " + nodeId + " " + timeout);

        stTime = System.currentTimeMillis();
        lastLeaderPing = 0;

        String color = "";
        if (nodeId == 1) color = "A";
        if (nodeId == 2) color = "B";
        if (nodeId == 3) color = "C";
        color = "  " + color + "  ";
        while (true) {
            int num = selector.select();
            long curTime = System.currentTimeMillis() - stTime;
            String tmr = String.valueOf(curTime) + "   ";

            System.err.println(tmr + color + "num: " + num);

            if (System.currentTimeMillis() >= lastLeaderPing + timeout)
                status = Status.FOLLOWER;


            for (Iterator <Map.Entry<Integer, SelectionKey>> it = friendsKeys.entrySet().iterator(); it.hasNext(); ) {
                SelectionKey key = it.next().getValue();
                if (!key.isValid())
                    it.remove();
            }

            if ((leaderId == -1 || System.currentTimeMillis() >= lastLeaderPing + timeout) && status == Status.FOLLOWER) {
                System.err.println(tmr + "CANDIDATE" + color);
                logHandler.reWriteTerm(logHandler.getTerm() + 1);
                lastLeaderPing = System.currentTimeMillis();
                status = Status.CANDIDATE;
                voteForMe = 1;
                termVote = true;
                for (Map.Entry<Integer, SelectionKey> entry : friendsKeys.entrySet()) {
                    SocketChannel sc = (SocketChannel) entry.getValue().channel();
                    if (sc.isConnected()) {
                        writeMessage(entry.getValue(), REQUEST_VOTE + " " + curTermId + " " + nodeId + " " + logHandler.getLastIndex() + " " + logHandler.getLastTerm());
                    }
                }
            }



            if (curTime >= lastConnect + timeout) {
                for (Map.Entry<Integer, Integer> entry : ports.entrySet()) {
                    int id = entry.getKey();
                    if (id > nodeId) {
                        SelectionKey tmpKey = friendsKeys.get(id);
                        if (tmpKey != null) tmpKey.cancel();
                        if (!isReady(tmpKey)) {
                            System.err.println(tmr + "connectTo " + color);
                            connectToNode(id);
                        }
                    }
                }
                lastConnect = curTime;
            }

            Set < SelectionKey > selectedKeys = selector.selectedKeys();

            Iterator < SelectionKey > it = selectedKeys.iterator();

            while (it.hasNext()) {
                SelectionKey key = it.next();
                it.remove();
                System.err.println(tmr + color + "key ready " + key.readyOps() + " int " + key.interestOps());

                if ((key.readyOps() & SelectionKey.OP_ACCEPT) == SelectionKey.OP_ACCEPT) {
                    System.err.println(tmr + color + "ACCEPT : ");

                    ServerSocketChannel scc = (ServerSocketChannel) key.channel();
                    SocketChannel sc = scc.accept();
                    if (sc == null) {
                        System.err.println(tmr + color + "something strange!");
                        assert(false);
                        continue;
                    }
                    sc.configureBlocking(false);
                    SelectionKey tmpKey = sc.register(selector, SelectionKey.OP_READ);
                    tmpKey.attach(new ChannelHelper(-1));
                }
                if ((key.readyOps() & SelectionKey.OP_CONNECT) == SelectionKey.OP_CONNECT) {
                    System.err.println(tmr + color + "CONNECT : ");
                    SocketChannel scNew = (SocketChannel) key.channel();
                    try {
                        assert(scNew.finishConnect());
                    } catch (IOException ignored) {
                        assert(false);
                    }
                    assert(isReady(key));
                    SelectionKey tmpKey = scNew.register(selector, SelectionKey.OP_READ, key.attachment());
                    writeMessage(tmpKey, "node " + nodeId);
                }

                if ((key.readyOps() & SelectionKey.OP_WRITE) == SelectionKey.OP_WRITE) {
                    System.err.println(tmr + color + "WRITE EVENT ");
                    SocketChannel sc = (SocketChannel) key.channel();
                    ByteBuffer buffer = ((ChannelHelper) key.attachment()).getBuffer();
                    buffer.flip();
                    int cntByte = sc.write(buffer);
                    ///assert (cntByte > 0);
                    buffer.compact();
                    if (buffer.position() == 0) {
                        sc.register(selector, key.interestOps() ^ SelectionKey.OP_WRITE, key.attachment());
                    }
                }
                if ((key.readyOps() & SelectionKey.OP_READ) == SelectionKey.OP_READ) {
                    System.err.println(tmr + color + "READ EVENT ");
                    ByteBuffer tmpBuffer = ByteBuffer.allocate(1024);
                    SocketChannel sc = (SocketChannel) key.channel();
                    int retValue = sc.read(tmpBuffer);
                    System.err.println("number read bytes " + retValue);
                    ChannelHelper helper = (ChannelHelper) key.attachment();
                    if (retValue == -1) {
                        if (helper.getNodeId() != -1) {
                            sc.close();
                            key.cancel();
                        }
                    } else {
                        tmpBuffer.flip();
                        byte [] g = new byte[tmpBuffer.remaining()];
                        tmpBuffer.get(g);
                        String tmp = new String(g, Charset.forName("UTF-8"));
                        System.err.println(tmr + "read result " + color + ":" + tmp);
                        helper.getsBuilder().append(tmp);
                        handleSocket(key);
                    }
                }
            }
        }
    }
}

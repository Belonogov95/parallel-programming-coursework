package com.company;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
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

public class DkvsNode {
    private Selector selector;
    private int myId;
    private int myPort;
    private Map < String, String > dataStorage;
    private int commitIndex;
    private int leaderId;
    private Map < Integer, SelectionKey > socketChannels;
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


    class WakeUpMe implements Runnable {
        int timeout;

        public WakeUpMe(int timeout) {
            this.timeout = timeout;
        }

        @Override
        public void run() {
            try {
                Thread.sleep(timeout);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            selector.wakeup();
        }
    }

    void wakeUpMe(int timeout) {
        new Thread(new WakeUpMe(timeout)).start();
    }

    void writeMessage(SelectionKey key, String message) throws ClosedChannelException {
        message = message + System.lineSeparator();
        ChannelHelper helper = (ChannelHelper)key.attachment();
        helper.getBuffer().put(message.getBytes(Charset.forName("UTF-8")));
        SocketChannel sc = (SocketChannel)key.channel();

        sc.register(selector, key.readyOps() | SelectionKey.OP_WRITE, key.attachment());
    }

    private void handleSocket(SelectionKey key) throws ClosedChannelException {
        ChannelHelper helper = (ChannelHelper) key.attachment();
        StringBuilder buffer = helper.getsBuilder();
        int pos = buffer.indexOf(System.lineSeparator());
        if (pos == -1) return;

        String query = buffer.substring(0, pos);
        int space = query.indexOf(" ");

        assert(space != -1);

        String command = query.substring(0, space);
        String args = query.substring(space).trim();
        if (command.equals(NODE)) {
            assert(helper.getId() == -1);
            int id = Integer.valueOf(args.trim());
            helper.setId(id);
            socketChannels.put(id, key);
            writeMessage(key, ACCEPTED);
        }
        if (command.equals(GET)) {
            assert(helper.getId() == -1);
            String res = dataStorage.get(args);
            if (res == null)
                writeMessage(key, NOT_FOUND);
            else
                writeMessage(key, VALUE + " " + query + " " + res);
        }
        if (command.equals(SET)) {
            assert(helper.getId() == -1);
            if (leaderId != -1) {
                writeMessage(key, STORED);
                writeMessage(socketChannels.get(leaderId), TO_LEADER + " " + command);
            }
        }
        if (command.equals(DELETE)) {
            assert(helper.getId() == -1);
            String res = dataStorage.get(args);
            if (leaderId != -1) {
                if (res == null)
                    writeMessage(key, NOT_FOUND);
                else
                    writeMessage(key, DELETED);
                writeMessage(socketChannels.get(leaderId), TO_LEADER + " "  + command);
            }
        }

        if (command.equals(TO_LEADER)) {
            if (leaderId == myId) {
                int nextSpace = args.indexOf(" ");
                command = args.substring(0, nextSpace);
                args = args.substring(nextSpace).trim();
                if (command.equals(DELETE)) {

                }
            }
        }

        helper.setsBuilder(new StringBuilder(buffer.substring(pos + System.lineSeparator().length(), buffer.length())));
        handleSocket(key);
    }

    public DkvsNode(int myId, int myPort, int timeL, int timeR, int heartBeat, Map < Integer, Integer > allPorts) throws IOException {
        assert(timeL < timeR);
        assert(heartBeat * 2 <= timeL);

        this.myId = myId;
        this.myPort = myPort;
        dataStorage = new TreeMap<>();
        commitIndex = 0;
        leaderId = -1;


        selector = Selector.open();


        MyTimer timer = new MyTimer(selector);
        new Thread(timer).start();


        Random random = new Random();
        timer.addEvent(System.currentTimeMillis() + timeL + random.nextInt(timeR - timeL));

        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.configureBlocking(false);
        ServerSocket ss = ssc.socket();
        InetSocketAddress address = new InetSocketAddress(myPort);
        ss.bind(address);
        ssc.register(selector, SelectionKey.OP_ACCEPT);

        socketChannels = new TreeMap<>();

        for (int i = 0; i < socketChannels.size(); i++) {
            SocketChannel sc = SocketChannel.open();
            sc.configureBlocking(false);
            sc.connect(new InetSocketAddress(allPorts.get(i)));
            SelectionKey key = sc.register(selector, SelectionKey.OP_CONNECT);
            socketChannels.put(i, key);
            key.attach(i);
        }


        while (true) {
            int num = selector.select();

            Set selectedKeys = selector.selectedKeys();

            Iterator it = selectedKeys.iterator();

            while (it.hasNext()) {
                SelectionKey key = (SelectionKey)it.next();
                if ((key.readyOps() & SelectionKey.OP_ACCEPT) == SelectionKey.OP_ACCEPT) {
                    ServerSocketChannel scc = (ServerSocketChannel) key.channel();
                    SocketChannel sc = scc.accept();
                    sc.configureBlocking(false);

                    SelectionKey tmpKey = sc.register(selector, SelectionKey.OP_READ);
                    tmpKey.attach(new ChannelHelper(-1));
                }
                if ((key.readyOps() & SelectionKey.OP_CONNECT) == SelectionKey.OP_CONNECT) {
                    int id = (Integer)key.attachment();
                    SocketChannel sc = (SocketChannel) socketChannels.get(id).channel();
                    if (sc.socket().isConnected()) {
                        sc.socket().close();
                    }
                    sc = (SocketChannel) key.channel();
                    sc.configureBlocking(false);
                    SelectionKey tmpKey = sc.register(selector, SelectionKey.OP_READ, new ChannelHelper(id));
                    writeMessage(tmpKey, "node " + myId + System.lineSeparator());
                    socketChannels.put(id, tmpKey);
                }

                if ((key.readyOps() & SelectionKey.OP_WRITE) == SelectionKey.OP_WRITE) {
                    SocketChannel sc = (SocketChannel) key.channel();
                    ByteBuffer buffer =((ChannelHelper)key.attachment()).getBuffer();
                    buffer.flip();
                    int cntByte = sc.write(buffer);
                    assert(cntByte > 0);
                    buffer.compact();
                    if (buffer.position() == 0) {
                        sc.register(selector, key.readyOps() ^ SelectionKey.OP_WRITE, key.attachment());
                    }
                }
                if ((key.readyOps() & SelectionKey.OP_READ) == SelectionKey.OP_READ) {
                    ByteBuffer tmpBuffer= ByteBuffer.allocate(1024);
                    SocketChannel sc = (SocketChannel) key.channel();
                    sc.read(tmpBuffer);
                    ChannelHelper helper = (ChannelHelper) key.attachment();
                    helper.getsBuilder().append(new String(tmpBuffer.array(), Charset.forName("UTF-8")));
                    handleSocket(key);
                }
                it.remove();
            }
        }
    }
}

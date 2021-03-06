package com.company;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class Main {
    private int ports[];
    private ByteBuffer echoBuffer = ByteBuffer.allocate(1024);

    public Main(int ports[]) throws IOException {
        this.ports = ports;

        configure_selector();
    }

    private void configure_selector() throws IOException {
        // Create a new selector
        Selector selector = Selector.open();
        // Open a listener on each port, and register each one
        // with the selector

        for (int i = 0; i < ports.length; ++i) {
            ServerSocketChannel ssc = ServerSocketChannel.open();
            ssc.configureBlocking(false);

            ServerSocket ss = ssc.socket();

            InetSocketAddress address = new InetSocketAddress(ports[i]);

            ss.bind(address);

            SelectionKey key = ssc.register(selector, SelectionKey.OP_ACCEPT);

            System.out.println("Going to listen on " + ports[i]);
        }

        while (true) {
            int num = selector.select();

            Set selectedKeys = selector.selectedKeys();
            Iterator it = selectedKeys.iterator();

            while (it.hasNext()) {
                SelectionKey key = (SelectionKey) it.next();

                if ((key.readyOps() & SelectionKey.OP_ACCEPT) == SelectionKey.OP_ACCEPT) {

                    // Accept the new connection

                    ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
                    SocketChannel sc = ssc.accept();
                    sc.configureBlocking(false);

                    SelectionKey newKey = sc.register(selector, SelectionKey.OP_READ);
                    it.remove();

                    System.out.println("Got connection from " + sc);
                } else if ((key.readyOps() & SelectionKey.OP_READ) == SelectionKey.OP_READ) {
                    SocketChannel sc = (SocketChannel) key.channel();

                    int bytesEchoed = 0;
                    while (true) {
                        echoBuffer.clear();

                        int number_of_bytes = sc.read(echoBuffer);

                        if (number_of_bytes <= 0) {
                            break;
                        }

                        echoBuffer.flip();

                        sc.write(echoBuffer);
                        bytesEchoed += number_of_bytes;
                    }

                    System.out.println("Echoed " + bytesEchoed + " from " + sc);

                    it.remove();
                }

            }
        }
    }

    static public void main(String args[]) {

        if (args.length == 1) {

        }
        ArrayList<Integer> listIp = new ArrayList<>();
        listIp.add(4444);
        int timeout = 100;
//        try (BufferedWriter bf = new BufferedWriter(new FileWriter("test.txt"))){
//            bf.write("yyyy!!!!");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        //System.out.println(Integer.valueOf("   010   ".trim()));

        //System.out.println(System.currentTimeMillis());

//        for (Integer x: listIp) {
//            new DkvsNode(x, timeout);
//        }
    }
}
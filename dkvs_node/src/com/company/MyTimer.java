package com.company;

import java.nio.channels.Selector;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Created by belonogov on 5/23/16.
 */
public class MyTimer implements Runnable  {
    private Selector selector;
    private PriorityBlockingQueue < Long > queue;

    public MyTimer(Selector selector) {
        this.selector = selector;
        queue = new PriorityBlockingQueue < Long >(1, new Comparator<Long>() {
            @Override
            public int compare(Long a, Long b) {
                if (a > b) return -1;
                if (a < b) return 1;
                return 0;
            }
        });
        queue.put((long) 1e18);
    }


    public void addEvent(long tmr) {
        queue.put(tmr);
    }

    @Override
    public void run() {
        while (true) {
            long curTime = System.currentTimeMillis();
            for (;  queue.peek() < curTime;) {
                assert(!queue.isEmpty());
                queue.poll();
                selector.wakeup();
            }
            long toNextEvent = queue.peek() - curTime;
            try {
                Thread.sleep(toNextEvent);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}

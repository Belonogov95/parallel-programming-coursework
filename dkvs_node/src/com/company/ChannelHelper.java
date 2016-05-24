package com.company;

import java.nio.ByteBuffer;

/**
 * Created by belonogov on 5/23/16.
 */
public class ChannelHelper {
    private Integer id;
    private ByteBuffer buffer;
    private StringBuilder sBuilder;

    public ChannelHelper(Integer id) {
        this.id = id;
        buffer = ByteBuffer.allocate(1024);
        sBuilder = new StringBuilder();
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public void setsBuilder(StringBuilder sBuilder) {
        this.sBuilder = sBuilder;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }
    public StringBuilder getsBuilder() {
        return sBuilder;
    }
}

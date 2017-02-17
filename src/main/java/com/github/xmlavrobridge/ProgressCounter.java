package com.github.xmlavrobridge;

import java.io.*;

public class ProgressCounter extends FilterInputStream {
    long progress = 0;

    @Override
    public long skip(long n) throws IOException {
        progress += n;
        return super.skip(n);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int read =  super.read(b, off, len);
        progress += read;
        return read;
    }

    @Override
    public int read() throws IOException {
        int red = super.read();
        progress += red;
        return red;
    }

    public ProgressCounter(InputStream in) {
        super(in);
    }

    public long getProgress () {
        return progress;
    }
}
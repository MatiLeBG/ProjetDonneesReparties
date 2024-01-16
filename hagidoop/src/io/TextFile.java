package io;

import java.io.BufferedWriter;
// import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
// import java.nio.Buffer;

import interfaces.FileReaderWriter;
import interfaces.KV;

public class TextFile implements FileReaderWriter {
    
    private int index;
    private String fileName;

    private transient BufferedWriter writer;
    private transient InputStreamReader reader;

    public TextFile(String fname) {
        this.index = 0;
        this.fileName = fname;
    }

    public TextFile() {
        index = 0;
    }

    @Override
    public void open(String mode) {
        try {
            if (mode.equals("read")) {
                reader = new InputStreamReader(new FileInputStream(fileName), "UTF-8");
            } else if (mode.equals("write")) {
                writer = new BufferedWriter(new FileWriter(fileName));
            } else {
                throw new IllegalArgumentException("Invalid mode: " + mode);
            }
            index = 0;
        } catch (IOException e) {
            System.err.println("Error opening file: " + e.getMessage());
        }
    }

    @Override
    public void close() {
        try {
            if (reader != null) {
                reader.close();
            }
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing file: " + e.getMessage());
        }
    }

    @Override
    public long getIndex() {
        return index;
    }

    @Override
    public String getFname() {
        return fileName;
    }

    @Override
    public void setFname(String fname) {
        fileName = fname;
    }

    public KV read() {
        try {
            if (reader == null) {
                throw new IllegalStateException("Reader not initialized");
            }
            String line = "";

            int c = reader.read();
            KV kv = null;
            while (c != -1) {
                if (c == '\n') {
                    break;
                }
                index++;
                line += (char) c;
                c = reader.read();
            }
            if (c == -1 ) {
                index = -1;
            }
            if (line.length() > 0) {
                kv = new KV();
                kv.k = "";
                kv.v = line;
            }
            return kv;
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
            return null;
        }
    }

    public void write(KV record) {
        try {
            if (writer == null) {
                throw new IllegalStateException("Writer not initialized");
            }
            writer.write(record.v);
            writer.newLine();
            index++;
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
        }
    }
}

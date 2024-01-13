package implementations;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.Buffer;

import interfaces.FileReaderWriter;
import interfaces.KV;

public class TextFile implements FileReaderWriter {
    
    private int index;
    private String fileName;

    private BufferedWriter writer;
    private InputStreamReader reader;

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

    public long getIndex() {
        return index;
    }

    public String getFname() {
        return fileName;
    }

    public void setFname(String fname) {
        fileName = fname;
    }

    public KV read() {
        return null;
    }

    public void write(KV record) {
        
    }
}

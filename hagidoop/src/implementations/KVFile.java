package implementations;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import interfaces.FileReaderWriter;
import interfaces.KV;

public class KVFile implements FileReaderWriter {
    
    private int index;
    private String fileName;

    private BufferedWriter writer;
    private InputStreamReader reader;

    public KVFile(String fname) {
        this.index = 0;
        this.fileName = fname;
    }

    public KVFile() {
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

    public void setIndex(int index) {
        this.index = index;
    }
    
    @Override
    public KV read() {
        try {
            StringBuilder sb = new StringBuilder();
            int c;
            while ((c = reader.read()) != -1) {
                if (c == '\n') {
                    break;
                }
                sb.append((char) c);
            }
            if (c == -1) {
                return null;
            }
            index++;
            String[] line = sb.toString().split(KV.SEPARATOR);
            return new KV(line[0], line[1]);
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
            return null;
        }
    }

    @Override
    public void write(KV record) {
        try {
            writer.write(record.toString());
            writer.newLine();
            index++;
        } catch (IOException e) {
            System.err.println("Error writing file: " + e.getMessage());
        }
    }

    public String getFname() {
        return fileName;
    }

    public void setFname(String fname) {
        fileName = fname;
    }
}

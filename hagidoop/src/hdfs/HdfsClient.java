package hdfs;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import implementations.*;
import interfaces.FileReaderWriter;

public class HdfsClient {

    private static final String ADRESSES_PORTS = "../config/adresses.txt"; // fichier texte contenant les adresses et ports
    private static List<String> adress = new ArrayList<>();
    private static List<String> port = new ArrayList<>();
    private static List<Integer> server_indexes = new ArrayList<>();

    private static void usage() {
        System.out.println("Usage: java HdfsClient read <file>");
        System.out.println("Usage: java HdfsClient write <txt|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }
    
    public static String addToFileName(int i, String filePath) {
        String directory = filePath.substring(0, filePath.lastIndexOf("/") + 1);
        String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
        String extension = fileName.substring(fileName.lastIndexOf("."));
        String nameWithoutExtension = fileName.substring(0, fileName.lastIndexOf("."));
        return directory + nameWithoutExtension + "_" + i + extension;
    }

    public static void retrieveSocketAdress (String socket_file) {
        try (BufferedReader lecteur = new BufferedReader(new FileReader(socket_file))) {
            String ligne;
            while ((ligne = lecteur.readLine()) != null) {
                String[] part = ligne.split(" ");
                adress.add(part[0]);
                port.add(part[1]);
                server_indexes.add(Integer.parseInt(part[2]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void HdfsDelete(String fname) {
        try {
            for (int i = 0; i < adress.size(); i++) {
                String fileName = addToFileName(server_indexes.get(i), fname) + "\n";
                Socket socket = new Socket(adress.get(i), Integer.parseInt(port.get(i)));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                System.out.println("2" + " ; " + fileName);
                out.println("2" + " ; " + fileName);
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void HdfsWrite(int fmt, String fname) {
        try {
            File file = new File(fname);
            if (file.exists()) {
                int fileSize = (int) file.length();
                int sectionSize = fileSize / adress.size();
                int currentSectionSize = 0;

                FileReaderWriter readerWriter;

                switch (fmt) {
                    case FileReaderWriter.FMT_TXT:
                        readerWriter = new TextFile();
                        break;
                    case FileReaderWriter.FMT_KV:
                        readerWriter = new KVFile();
                        break;
                    default:
                        break;
                }
                for (int i = 0; i < adress.size(); i++) {
                    String fileName = addToFileName(server_indexes.get(i), fname) + "\n";
                    Socket socket = new Socket(adress.get(i), Integer.parseInt(port.get(i)));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    String section = "";
                    BufferedReader reader = new BufferedReader(new FileReader(fname));
                    int character;
                    while ((character = reader.read()) != -1 && currentSectionSize <= (i + 1) * sectionSize) {
                        char c = (char) character;
                        System.out.println(c);
                        if (currentSectionSize + 1 <= sectionSize) {
                            section += c;
                            System.out.println(section);
                            currentSectionSize++;
                        } else {
                            out.println("1" + " ; " + fileName + "\n" + section);
                            section = "";
                        }
                    }
                    out.println("1" + " ; " + fileName + "\n" + section);
                    reader.close();
                }
            } else {
                System.out.println("Le fichier " + fname + " n'existe pas.");
                System.exit(1);
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void HdfsRead(String fname) {
        try {
            for (int i = 0; i < adress.size(); i++) {
                String fileName = fname + "_" + server_indexes.get(i) + "\n";
                Socket socket = new Socket(adress.get(i), Integer.parseInt(port.get(i)));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                out.println("2" + " | " + fileName);
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            usage();
            return;
        }

        String command = args[0];
        String fname = args[1];

        retrieveSocketAdress(ADRESSES_PORTS);
        
        switch (command) {
            case "read":
                HdfsRead(fname);
                break;
            case "write":
                if (args.length < 3) {
                    usage();
                    return;
                }
                int fmt = args[2].equals("txt") ? 0 : 1; // 0 pour txt, 1 pour kv
                HdfsWrite(fmt, fname);
                break;
            case "delete":
                HdfsDelete(fname);
                break;
            default:
                usage();
                break;
        }
    }
}
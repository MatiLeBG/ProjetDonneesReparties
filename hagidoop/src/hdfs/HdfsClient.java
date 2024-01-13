package hdfs;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class HdfsClient {

    private static final int SERVER_PORT = 5000; // Port du serveur HdfsServer
    private static final String SERVER_HOST = "localhost"; // Hôte du serveur HdfsServer
    private static final String ADRESSES_PORTS = "../config/adresses.txt"; // fichier texte contenant les adresses et ports
    private static List<String> adress = new ArrayList<>();
    private static List<String> port = new ArrayList<>();


    private static void usage() {
        System.out.println("Usage: java HdfsClient read <file>");
        System.out.println("Usage: java HdfsClient write <txt|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }

    public static void retrieveSocketAdress (String socket_file) {
        try (BufferedReader lecteur = new BufferedReader(new FileReader(socket_file))) {
            String ligne;
            while ((ligne = lecteur.readLine()) != null) {
                String[] part = ligne.split(" ");
                adress.add(part[0]);
                port.add(part[1]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void HdfsDelete(String fname) {
        try {
            Socket socket = new Socket(SERVER_HOST, SERVER_PORT);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println("DELETE " + fname);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void HdfsWrite(int fmt, String fname) {
        try {
            Socket socket = new Socket(SERVER_HOST, SERVER_PORT);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println("WRITE " + fmt + " " + fname);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void HdfsRead(String fname) {
        try {
            Socket socket = new Socket(SERVER_HOST, SERVER_PORT);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println("READ " + fname);
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
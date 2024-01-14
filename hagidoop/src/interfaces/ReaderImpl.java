package interfaces;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;

import daemon.HdfsServer;

public class ReaderImpl implements Reader {

    private Socket ssock;
    private int nbPort;
    static byte[] buffer = new byte[1024];

    public ReaderImpl(Socket s, int nb) {
        this.ssock = s;
        nbPort = nb;
    }

    public static void main(String[] args) throws IOException {
        int nbPort = Integer.parseInt(args[0]);
        ServerSocket ssock = new ServerSocket(nbPort);
        while (true) {
            new Thread(new HdfsServer(ssock.accept(), nbPort)).start();
        }
    }

    public KV read() {
        KV kv = new KV("null", "null");
        try {
            ObjectInputStream ois = new ObjectInputStream(
                    ssock.getInputStream());
            kv = (KV) ois.readObject();
            ssock.close();
        } catch (IOException | ClassNotFoundException ex) {
            ex.printStackTrace();
        }
        return kv;
    }

}

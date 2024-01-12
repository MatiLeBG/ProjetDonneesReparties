package daemon;


import java.net.*;
import java.io.*;

public class HdfsServer implements Runnable {
    private Socket ssock;
    private int nbPort;
    static byte[] buffer = new byte[1024];
	public HdfsServer (Socket s, int nb) { this.ssock = s; nbPort = nb;}
    

    public static void main(String[] args) throws IOException {
        int nbPort = Integer.parseInt(args[0]);
        ServerSocket ssock = new ServerSocket(nbPort);  
        while(true) { new Thread(new HdfsServer(ssock.accept(), nbPort)).start(); }   
    }   

    public void run(){
        try {
            int nb = this.nbPort;
            System.out.println(nb);
            ObjectInputStream ois = new ObjectInputStream(
            ssock.getInputStream());
            Integer v = 0;
            v = (Integer) ois.readObject();
            System.out.println(v);
            int p = nb + v;
            System.out.println("Received person: "+ p);
            ssock.close();
        } catch (IOException | ClassNotFoundException ex) {
			ex.printStackTrace();
        }
    }
}

package io;

import interfaces.NetworkReaderWriter;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;

import interfaces.KV;

public class NetworkReaderWriterImpl implements NetworkReaderWriter{

    private transient Socket client;
    private int port;
    private String adress;

    private transient ServerSocket server;
    
    private transient ObjectOutputStream output;
    private transient ObjectInputStream input;

    public static final int PORT = 8000;
    public NetworkReaderWriterImpl(Socket socket, int port, String adress){
            this.client = socket;
            this.port = port;
            this.adress = adress;

            try {
                output = new ObjectOutputStream(client.getOutputStream());
                input = new ObjectInputStream(client.getInputStream());
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
    }
    
    public KV read(){
       
            
            
    }

    public void write(KV remoteKV){

    }

    public void openServer(){
           
            try {
            
            this.server =  new ServerSocket(PORT);
            
        
            } catch (IOException e) {
                System.out.println("Une erreur est survenue lors de l'écoute sur le port " + PORT);
        }
            
    }

    public void openClient(){
        try {
            this.client = new Socket("localhost",PORT);
        
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public void closeClient(){
        try {
            this.client.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public void closeServer() {
        try {
            this.server.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public NetworkReaderWriter accept(){

        Socket client;
        try {
             client = this.server.accept();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return new NetworkReaderWriterImpl(client, port, adress);

    }

    class NetworkThread extends Thread {
        Socket ssock;
        public NetworkThread(Socket s) {
        this.ssock = s;
        }

        public void run() {
        try {
        ObjectInputStream ois = new ObjectInputStream(
        ssock.getInputStream());

        KV readKV = (KV) ois.readObject();
       
        //ssock.close();
        } catch (Exception e) {
        System.out.println("An error has occurred ...");
        }

    }
}
}

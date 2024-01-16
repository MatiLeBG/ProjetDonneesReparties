package io;

import interfaces.NetworkReaderWriter;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.io.PrintWriter;
import java.io.BufferedReader;

import interfaces.KV;

public class NetworkReaderWriterImpl implements NetworkReaderWriter {

    private transient Socket client;
    private int port;
    private String adress;

    private transient ServerSocket server;
    private transient PrintWriter output;
    private transient BufferedReader input;
    int bufferSize = 16384;

    public static final int PORT = 8000;

    public NetworkReaderWriterImpl(int port, String adress) {
        this.port = port;
        this.adress = adress;
    }

    public NetworkReaderWriterImpl(Socket socket, int port, String adress) {
        this.client = socket;
        this.port = port;
        this.adress = adress;

        try {
            output = new PrintWriter(socket.getOutputStream(), true);
            input = new BufferedReader(new InputStreamReader(socket.getInputStream()), bufferSize);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public KV read() {
        KV remoteKV = null;
        try {
            String line = input.readLine();
            
            if (line != null) {
                remoteKV = KV.fromString(line);
            }
            return remoteKV;
        } catch (IOException e) {
            e.printStackTrace();
            return remoteKV;
        }
    }

    public void write(KV remoteKV) {
        output.println(remoteKV.toString());
    }

    public void openServer() {
        try {
            this.server = new ServerSocket(PORT);
        } catch (IOException e) {
            System.out.println("Une erreur est survenue lors de l'écoute sur le port " + PORT);
        }

    }

    public void openClient() {
        try {
            this.client = new Socket("localhost", PORT);
            output = new PrintWriter(client.getOutputStream(), true);
            input = new BufferedReader(new InputStreamReader(client.getInputStream()));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void closeClient() {
        try {
            this.client.close();
            output.close();
            input.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void closeServer() {
        try {
            this.server.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public NetworkReaderWriter accept() {

        Socket client;
        try {
            client = this.server.accept();
        } catch (IOException e) {
            client = null;
            e.printStackTrace();
        }
        return new NetworkReaderWriterImpl(client, port, adress);

    }
}

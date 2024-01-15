package io;

import interfaces.KV;
import interfaces.Reader;

public class Adaptateur implements Reader {

    private NetworkReaderWriterImpl serveur;
    private int nbClients;

    public Adaptateur(NetworkReaderWriterImpl serveur, int nbClients) {
        this.serveur = serveur;
        this.nbClients = nbClients;
    }

    public KV read() {
        return new KV("", "");
    }

    public void getKV() {
        int i = 0;
        while (i < nbClients) {

        }
    }

    public class AdaptateurThread implements Runnable {

        private int num;
        private NetworkReaderWriterImpl serveur;

        public AdaptateurThread(int numeroServer, NetworkReaderWriterImpl serveur) {
            this.num = numeroServer;
            this.serveur = serveur;
        }

        public void run() {
            NetworkReaderWriterImpl client = serveur.accept();
        }
    }
}

package daemon;

import interfaces.KV;
import interfaces.NetworkReaderWriter;
import interfaces.Reader;
import io.NetworkReaderWriterImpl;

import java.util.ArrayDeque;
import java.util.Queue;

public class Adaptater extends Thread implements Reader {

    private int nbWorker;
    private NetworkReaderWriterImpl networkReaderWriter;
    private NetworkReaderWriter netWork;
    static Queue<KV> queue = new ArrayDeque<>();

    public Adaptater(int nbWorkers) {
        networkReaderWriter = new NetworkReaderWriterImpl(2000, "localhost");
        nbWorker = nbWorkers;
    }

    public KV read() {
        KV returnKV = null;
        if (!queue.isEmpty()) {
            returnKV = queue.poll();
        }
        return returnKV;
    }

    public void run() {
        try {
            networkReaderWriter.openServer();
            for (int i = 0; i < nbWorker; i++) {
                netWork = networkReaderWriter.accept();
                FillKVQueue fillQueue = new FillKVQueue(netWork, queue, i);
                fillQueue.start();
            }
            networkReaderWriter.closeServer();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}

class FillKVQueue extends Thread {
    private NetworkReaderWriter network;
    private Queue<KV> queue;
    private int id;

    public FillKVQueue(NetworkReaderWriter network, Queue<KV> queue, int id) {
        this.network = network;
        this.queue = queue;
        this.id = id;
    }

    public void run() {
        try {
            KV kv;
            System.out.println("Retrieving [WORKER-" + id + "] file KV...");
            while ((kv = network.read()) != null) {
                queue.add(kv);
            }
            System.out.println("Retrieved [WORKER-" + id + "] file KV");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
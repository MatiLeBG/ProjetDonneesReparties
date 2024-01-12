package daemon;

import java.rmi.Naming;
import java.rmi.Remote;
import java.rmi.RemoteException;

import interfaces.FileReaderWriter;
import interfaces.Map;
import interfaces.NetworkReaderWriter;

public class WorkerImpl implements Worker {

    public WorkerImpl(){

    }

    public void runMap (Map m, FileReaderWriter reader, NetworkReaderWriter writer) throws RemoteException {
        m.map(reader,writer);
    }

    public static void main(String args[]) {
    try {
    // Create an instance of the server object
    WorkerImpl obj = new WorkerImpl();
    // Register the object with the naming service
    Naming.rebind("//localhost:8000/test", obj);
    System.out.println("WorkerImpl " + " bound in registry");
    } catch (Exception exc) {
        exc.printStackTrace();
    }
}
}
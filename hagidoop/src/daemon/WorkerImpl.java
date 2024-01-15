package daemon;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

import interfaces.FileReaderWriter;
import interfaces.Map;
import interfaces.NetworkReaderWriter;

public class WorkerImpl extends UnicastRemoteObject implements Worker, Serializable {

    private static final String ADRESSES_PORTS = "config/adressesWorker.txt"; // fichier texte contenant les
                                                                              // adresses et ports
    private static int server_index;

    private String adress;

    private int port;

    public WorkerImpl(String adr, int prt) throws java.rmi.RemoteException{
        adress = adr;
        port = prt;
    }

    public void runMap(Map m, FileReaderWriter reader, NetworkReaderWriter writer) throws java.rmi.RemoteException {
        writer.openClient();
        m.map(reader, writer);
        writer.closeClient();
    }

    public void test() throws java.rmi.RemoteException {
        System.out.println("Ce test a été éffectué avec succès");
    }

    public static void main(String args[]) {

        String adress = args[0];
        int port = Integer.parseInt(args[1]);

        server_index = addAdressPort(adress, port, ADRESSES_PORTS);

        
         Runtime.getRuntime().addShutdownHook(new Thread(() -> {
         System.out.println("Interruption détectée. Nettoyage en cours...");
         // Appeler la méthode pour supprimer l'adresse et le port du fichier texte
         deleteAdressPort(adress, port, server_index, ADRESSES_PORTS);
         }));
         

        try {
            Registry registry = LocateRegistry.createRegistry(port);
            // Create an instance of the server object
            WorkerImpl obj = new WorkerImpl(adress, port);
            // Register the object with the naming service
            String serverName = adress + ":" + port + "/workerServer";
            System.out.println(serverName);
            Naming.rebind(serverName, obj);
            System.out.println("WorkerImpl " + " bound in registry");
        } catch (Exception exc) {
            exc.printStackTrace();
        } 
    }

    private static int addAdressPort(String adresse, int port, String cheminFichier) {
        List<String> adressesPorts = lireFichier(cheminFichier);
        int lineNumber = adressesPorts.size();
        String lineToAdd = adresse + " " + port + " " + lineNumber;

        if (adressesPorts.contains(lineToAdd)) {
            System.out.println(
                    "L'adresse " + adresse + " et le port " + port + " existent déjà dans le fichier " + cheminFichier);
            System.exit(1);
        }
        try (PrintWriter writer = new PrintWriter(new FileWriter(cheminFichier, true))) {
            writer.println(lineToAdd);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("L'adresse " + adresse + " et le port " + port + " ont été ajoutés à la ligne " + lineNumber
                + " du fichier " + cheminFichier);
        return lineNumber;
    }

    private static List<String> lireFichier(String cheminFichier) {
        List<String> lignes = new ArrayList<>();
        try (BufferedReader lecteur = new BufferedReader(new FileReader(cheminFichier))) {
            String ligne;
            while ((ligne = lecteur.readLine()) != null) {
                lignes.add(ligne);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lignes;
    }

    private static void deleteAdressPort(String address, int port, int server_index, String cheminFichier) {
        try {
            File file = new File(cheminFichier);
            List<String> lines = Files.readAllLines(file.toPath());
            String lineToRemove = address + " " + port + " " + server_index;
            lines.remove(lineToRemove);
            Files.write(file.toPath(), lines);
        } catch (IOException e) {
            System.out.println("Une erreur est survenue lors de la suppression de l'adresse et du port.");
        }
    }

    public String getAdress() {
        return this.adress;
    }

    public int getPort() {
        return this.port;
    }
}

package hdfs;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class HdfsServer {

    private static final String ADRESSES_PORTS = "../config/adresses.txt"; // fichier texte contenant les adresses et ports
    private static int server_index;
    
    public static void main(String[] args) {

        if (args.length < 2) {
            System.out.println("\n-----------------------------------\n");
            System.out.println("Usage : java HdfsServer address port");
            System.out.println("\n-----------------------------------\n");
            System.exit(1);
        }

        String adress = args[0];
        int port = Integer.parseInt(args[1]);

        server_index = addAdressPort(adress, port, ADRESSES_PORTS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Interruption détectée. Nettoyage en cours...");
            // Appeler la méthode pour supprimer l'adresse et le port du fichier texte
            deleteAdressPort(adress, port, ADRESSES_PORTS);
        }));

        try {
            ServerSocket serverSocket = new ServerSocket(port);
            while (!serverSocket.isClosed()) {
                Socket clientSocket = serverSocket.accept();
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                String command = in.readLine();
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                handleCommand(command);
            }
        } catch (IOException e) {
            System.out.println("Une erreur est survenue lors de l'écoute sur le port " + port);
        } finally {
            deleteAdressPort(adress, port, ADRESSES_PORTS);
        }
    }

    private static int addAdressPort(String adresse, int port, String cheminFichier) {
        List<String> adressesPorts = lireFichier(cheminFichier);
        String lineToAdd = adresse + " " + port;
        if (adressesPorts.contains(lineToAdd)) {
            System.out.println("L'adresse " + adresse + " et le port " + port + " existent déjà dans le fichier " + cheminFichier);
            System.exit(1);
        }
        int lineNumber = adressesPorts.size();
        try (PrintWriter writer = new PrintWriter(new FileWriter(cheminFichier, true))) {
            writer.println(lineToAdd);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("L'adresse " + adresse + " et le port " + port + " ont été ajoutés à la ligne " + lineNumber + " du fichier " + cheminFichier);
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

    private static void deleteAdressPort(String address, int port, String cheminFichier) {
        try {
            File file = new File(cheminFichier);
            List<String> lines = Files.readAllLines(file.toPath());
            String lineToRemove = address + " " + port;
            lines.remove(lineToRemove);
            Files.write(file.toPath(), lines);
        } catch (IOException e) {
            System.out.println("Une erreur est survenue lors de la suppression de l'adresse et du port.");
        }
    }

    private static void handleCommand(String command) {
        String[] parts = command.split(" ");
        String cmd = parts[0];
        switch (cmd) {
            case "READ":
                String readFileName = parts[1];
                readFile(readFileName);
                break;
            case "WRITE":
                String writeFileName = parts[2];
                int fmt = Integer.parseInt(parts[1]);
                writeFile(writeFileName, fmt);
                break;
            case "DELETE":
                String deleteFileName = parts[1];
                deleteFile(deleteFileName);
                break;
            default:
                System.out.println("Commande non reconnue: " + cmd);
                break;
        }
    }

    private static void readFile(String fileName) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
            reader.close();
        } catch (FileNotFoundException e) {
            System.out.println("Le fichier " + fileName + " n'a pas été trouvé.");
        } catch (IOException e) {
            System.out.println("Une erreur est survenue lors de la lecture du fichier " + fileName);
        }
    }
    
    private static void writeFile(String fileName, int fmt) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
            // TODO: Écrire dans le fichier en fonction du format (fmt)
            writer.close();
        } catch (IOException e) {
            System.out.println("Une erreur est survenue lors de l'écriture dans le fichier " + fileName);
        }
    }
    
    private static void deleteFile(String fileName) {
        File file = new File(fileName);
        if (file.delete()) {
            System.out.println("Le fichier " + fileName + " a été supprimé.");
        } else {
            System.out.println("La suppression du fichier " + fileName + " a échoué.");
        }
    }
}
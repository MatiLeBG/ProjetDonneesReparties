package hdfs;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * La classe HdfsServer représente un serveur dans le système de fichiers distribué Hadoop (HDFS).
 * Il écoute les connexions des clients et gère différentes commandes telles que la lecture, l'écriture et la suppression de fichiers.
 */
public class HdfsServer {

    private static final String ADRESSES_PORTS = "src/config/adresses.txt"; // fichier texte contenant les adresses et ports
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
            System.out.println("\n[SERVER" + "-" + server_index + "/" + adress + "-" + port + "] Interrupted. Cleaning...");
            // Appeler la méthode pour supprimer l'adresse et le port du fichier texte
            deleteAdressPort(adress, port, server_index, ADRESSES_PORTS);
        }));

        try {
            ServerSocket serverSocket = new ServerSocket(port);
            
            while (!serverSocket.isClosed()) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("\n[SERVER" + "-" + server_index + "/" + adress + "-" + port + "] Client connected");
                    HdfsServerThread thread = new HdfsServerThread(port, clientSocket);
                    thread.start();
                } catch (SocketException e) {
                    if (e.getMessage().equals("Socket closed")) {
                        System.out.println("Server socket closed");
                    } else {
                        e.printStackTrace();
                    }
                }
            }
            serverSocket.close();
        } catch (IOException e) {
            System.out.println("Une erreur est survenue lors de l'écoute sur le port " + port);
        }
    }

    /**
     * Ajoute une adresse et un port à un fichier spécifié.
     * 
     * @param adresse l'adresse à ajouter
     * @param port le port à ajouter
     * @param cheminFichier le chemin du fichier dans lequel ajouter l'adresse et le port
     * @return le numéro de ligne où l'adresse et le port ont été ajoutés
     */
    private static int addAdressPort(String adresse, int port, String cheminFichier) {
        List<String> adressesPorts = lireFichier(cheminFichier);
        int lineNumber = adressesPorts.size();
        String lineToAdd = adresse + " " + port + " " + lineNumber;

        if (adressesPorts.contains(lineToAdd)) {
            System.out.println("L'adresse " + adresse + " et le port " + port + " existent déjà dans le fichier " + cheminFichier); 
            System.exit(1);
        }
        try (PrintWriter writer = new PrintWriter(new FileWriter(cheminFichier, true))) {
            writer.println(lineToAdd);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("\nServer [SERVER-" + lineNumber + "/" + adresse + "-" + port + "] started\n");
        return lineNumber;
    }

    /**
     * Lit un fichier texte et retourne son contenu sous forme de liste de lignes.
     *
     * @param cheminFichier le chemin du fichier à lire
     * @return une liste de lignes du fichier
     */
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

    /**
     * Supprime une adresse et un port du fichier spécifié.
     * 
     * @param address l'adresse à supprimer
     * @param port le port à supprimer
     * @param index index du serveur
     * @param cheminFichier le chemin du fichier contenant les adresses et les ports
     */
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
}

class HdfsServerThread extends Thread {

    // private int port;
    private Socket clientSocket;

    public HdfsServerThread(int port, Socket clientSocket) {
        //this.port = port;
        this.clientSocket = clientSocket;
    }

    private static void readFile(String fileName, PrintWriter writer) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            String line;
            System.out.println("Envoi du fragment " + fileName + " au client...");
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                writer.println(line);
            }
            System.out.println("Fragment " + fileName + " envoyé au client.");
            reader.close();
        } catch (FileNotFoundException e) {
            System.out.println("Le fichier " + fileName + " n'a pas été trouvé.");
        } catch (IOException e) {
            System.out.println("Une erreur est survenue lors de la lecture du fichier " + fileName);
        }
    }
    
    private static void writeFile(String fileName, BufferedReader reader) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
            String line = reader.readLine();
            while (line != null) {
                writer.write(line);
                String nextLine = reader.readLine();
                if (nextLine != null) {
                    writer.newLine();
                }
                line = nextLine;
            }
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

    @Override
    public void run() {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);    
            String input = reader.readLine();
            String header = null;
            String fileName = null;
            if (input != null) {
                header = input.split(HdfsClient.HEADER_SEPARATOR)[0];
                fileName = input.split(HdfsClient.HEADER_SEPARATOR)[1];

                if (header == null) {
                    System.out.println("\nUne erreur est survenue lors de la lecture de la commande.\n");
                } else if (header.equals("0")) {
                        // Read file
                        System.out.println("\nReading file " + fileName + "...\n");
                        readFile(fileName, writer);
                } else if (header.equals("1")) {
                        // Write file
                        System.out.println("\nWriting to file " + fileName + "...\n");
                        writeFile(fileName, reader);
                } else if (header.equals("2")) {
                        // Delete file
                        System.out.println("\nDeleting file " + fileName + "...\n");
                        deleteFile(fileName);
                } else {
                        // Wrong header
                        writer.println("Wrong header");
                }
            }

            reader.close();
            writer.close();
            clientSocket.close();
        } catch (IOException e) {
            System.out.println("Une erreur est survenue lors de la lecture de la commande.");
        }
    }
}
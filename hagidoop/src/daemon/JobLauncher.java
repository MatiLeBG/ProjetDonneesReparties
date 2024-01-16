package daemon;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.rmi.Naming;
import java.util.ArrayList;
import java.util.List;

import application.Count;
import interfaces.FileReaderWriter;
import interfaces.MapReduce;
import interfaces.NetworkReaderWriter;
import io.KVFile;
import io.TextFile;
import io.NetworkReaderWriterImpl;
import hdfs.HdfsClient;

public class JobLauncher {

	private static final String ADRESSES_PORTS = "config/adressesWorker.txt"; // fichier texte contenant les
																				// adresses et ports
	// private static final String REDUCED_FILE_PATH = "data/reduced/";

	private static final String REDUCED_FILE_PATH = "../data/reduced/";

	private static List<String> adress = new ArrayList<>();
	private static List<String> port = new ArrayList<>();
	private static List<Integer> server_indexes = new ArrayList<>();

	private int format;
	private String fname;
	private MapReduce mr;

	public JobLauncher(MapReduce mr, int format, String fname) {
		this.format = format;
		this.fname = fname;
		this.mr = mr;

		startJob(mr, format, fname);
	}

	public static void startJob(MapReduce mr, int format, String fname) {

		try {
			retrieveWorkerAdress(ADRESSES_PORTS);
			System.out.println(adress.size());
			NetworkReaderWriter networkWriter;
			FileReaderWriter reader = null;
			System.out.println("Adresses : " + adress.size());
			Adaptater adaptater = new Adaptater(adress.size());
			adaptater.start();

			FileReaderWriter reduced = new KVFile();
			String fnameWoPath = fname.substring(fname.lastIndexOf("/") + 1);
			reduced.setFname(REDUCED_FILE_PATH + fnameWoPath);

			Thread[] workThreads = new Thread[adress.size()];

			// On parcoure la liste des workers
			for (int i = 0; i < adress.size(); i++) {
				String serverName = adress.get(i) + ":" + port.get(i) + "/workerServer";

				String fileName = HdfsClient.addToFileName(i, fname);
				File file = new File(fileName);
				System.out.println("Création du fichier " + fileName);

				if (file.createNewFile()) {
					System.out.println("Création du fichier " + fileName + " réussie !");
				} else {
					System.out.println("Le fichier " + fileName + " existe bien");
				}
				// System.out.println(fileName);

				Worker wi = (Worker) Naming.lookup(serverName);
				// System.out.println(wi.getAdress());

				if (format == FileReaderWriter.FMT_TXT) {
					System.out.println("Opération sur un fichier texte");
					reader = new TextFile(fileName);
				} else if (format == FileReaderWriter.FMT_KV) {
					System.out.println("Opération sur un fichier KV");
					reader = new KVFile(fileName);
				} else {
					System.out.println("Format non reconnu");
					System.exit(1);
				}
				networkWriter = new NetworkReaderWriterImpl(2000, "localhost");
				reader.open("read");

				// On lance les runMap de chaque worker dans un thread pour que ça soit en
				// simultané
				System.out.println("Mapping du worker : [WORKER-" + i + "]");
				workThreads[i] = new WorkInThread(wi, mr, reader, networkWriter);
				workThreads[i].start();
			}
			long startMapTime = System.nanoTime() / 1000000;
			for (int i = 0; i < adress.size(); i++) {
				try {
					workThreads[i].join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			long endMapTime = System.nanoTime() / 1000000;
			System.out.println("Temps d'exécution du map sur les workers : " + (endMapTime - startMapTime) + " ms");

			reduced.open("write");

			System.out.println("Reducing...");
			long startReduceTime = System.nanoTime() / 1000000;
			mr.reduce(adaptater, reduced);
			long endReduceTime = System.nanoTime() / 1000000;
			System.out.println("Reducing terminé");
			System.out.println("Temps d'exécution du reduce : " + (endReduceTime - startReduceTime) + " ms");
		} catch (Exception exc) {
			exc.printStackTrace();
		}

	}

	public static <T> void main(String args[])
			throws ClassNotFoundException, InstantiationException, IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException, SecurityException {

		MapReduce mr = (MapReduce) (Class.forName(args[0])).getConstructor().newInstance();
		retrieveWorkerAdress(ADRESSES_PORTS);
		int format = Integer.parseInt(args[1]);
		if (format == 0) {
			format = FileReaderWriter.FMT_TXT;
		} else if (format == 1) {
			format = FileReaderWriter.FMT_KV;
		} else {
			System.out.println("Format non reconnu");
			System.exit(1);
		}

		String fname = args[2];
		startJob((MapReduce) mr, format, fname);

	}

	public static void retrieveWorkerAdress(String socket_file) {
		try (BufferedReader lecteur = new BufferedReader(new FileReader(socket_file))) {
			String ligne;
			while ((ligne = lecteur.readLine()) != null) {
				String[] part = ligne.split(" ");
				adress.add(part[0]);
				port.add(part[1]);
				server_indexes.add(Integer.parseInt(part[2]));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

class WorkInThread extends Thread {
	private Worker wi;
	private MapReduce mr;
	private FileReaderWriter reader;
	private NetworkReaderWriter networkWriter;

	public WorkInThread(Worker wi, MapReduce mr, FileReaderWriter reader, NetworkReaderWriter networkWriter) {
		this.wi = wi;
		this.mr = mr;
		this.reader = reader;
		this.networkWriter = networkWriter;
	}

	public void run() {
		try {

			wi.runMap(mr, reader, networkWriter);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
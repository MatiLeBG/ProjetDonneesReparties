package daemon;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.rmi.Naming;
import java.util.ArrayList;
import java.util.List;

import application.Count;
import interfaces.FileReaderWriter;
import interfaces.MapReduce;
import interfaces.NetworkReaderWriter;

public class JobLauncher {

	private static final String ADRESSES_PORTS = "config/adressesWorker.txt"; // fichier texte contenant les
																				// adresses et ports
	private static List<String> adress = new ArrayList<>();
	private static List<String> port = new ArrayList<>();
	private static List<Integer> server_indexes = new ArrayList<>();

	public static void startJob(MapReduce mr, int format, String fname) {

		try {
			System.out.println(adress.size());
			for (int i = 0; i < adress.size(); i++) {
				String serverName = adress.get(i) + ":" + port.get(i) + "/workerServer";
				System.out.println(serverName);
				// On lance les map sur les workers
				WorkerImpl wi = (WorkerImpl) Naming.lookup(serverName);
				System.out.println(wi.getAdress());
				BufferedReader reader = new BufferedReader(new FileReader(fname));

				// Il faut implémenter les interfaces FileReaderWriter et NetworkReaderWriter
				// wi.runMap(mr,new FileReaderWriter(), new NetworkReaderWriter() );
				wi.test();
				// On lance aussi le reduce je crois
				// mr.reduce(reader, writer);
			}
		} catch (Exception exc) {
			exc.printStackTrace();

		}
	}

	public static <T> void main(String args[])
			throws ClassNotFoundException, InstantiationException, IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException, SecurityException {
		Class classe = Class.forName(args[0]);
		Constructor cst = classe.getConstructor();
		Object mr = cst.newInstance();
		retrieveWorkerAdress(ADRESSES_PORTS);
		int format = Integer.parseInt(args[1]);
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

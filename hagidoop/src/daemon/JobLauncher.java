package daemon;

import java.io.BufferedReader;
import java.io.File;
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
import io.KVFile;
import io.TextFile;

public class JobLauncher {

	private static final String ADRESSES_PORTS = "config/adressesWorker.txt"; // fichier texte contenant les
																				// adresses et ports
	private static List<String> adress = new ArrayList<>();
	private static List<String> port = new ArrayList<>();
	private static List<Integer> server_indexes = new ArrayList<>();

	private int format;
	private String fname;
	private MapReduce mr;
	public JobLauncher(MapReduce mr, int format, String fname){
		this.format = format;
		this.fname = fname;
		this.mr = mr;

		startJob(mr, format, fname);
	}
	public static void startJob(MapReduce mr, int format, String fname) {

		try {
			System.out.println(adress.size());
			for (int i = 0; i < adress.size(); i++) {
				String serverName = adress.get(i) + ":" + port.get(i) + "/workerServer";
				
				String extension = fname.substring(fname.lastIndexOf("."));
				String fileName =  fname.substring(0, fname.lastIndexOf("."));
				fileName = fileName + "-" + i + extension ;
				File file = new File(fileName);
				if (file.createNewFile()){
					System.out.println("feur");
				} else {
					System.out.println("le fichier " + fileName + " existe bien");
				}
				System.out.println(fileName);
				
				// On lance les map sur les workers
				Worker wi = (Worker) Naming.lookup(serverName);
				System.out.println(wi.getAdress());
			
				FileReaderWriter reader;

				if (format == FileReaderWriter.FMT_TXT){
					reader = new TextFile(fileName);
				} else {
					reader = new KVFile(fileName);
				}

				// Il faut implémenter les interfaces FileReaderWriter et NetworkReaderWriter
				wi.runMap(mr,reader,);
				
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

	public static String addToFileName(int i, String filePath) {
        String directory = filePath.substring(0, filePath.lastIndexOf("/") + 1);
        String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
        String extension = fileName.substring(fileName.lastIndexOf("."));
        String nameWithoutExtension = fileName.substring(0, fileName.lastIndexOf("."));
        return directory + nameWithoutExtension + "-" + i + extension;
    }
}

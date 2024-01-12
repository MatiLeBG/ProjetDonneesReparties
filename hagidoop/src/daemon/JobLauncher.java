package daemon;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.rmi.Naming;

import interfaces.FileReaderWriter;
import interfaces.MapReduce;
import interfaces.NetworkReaderWriter;

public class JobLauncher {

	public static void startJob (MapReduce mr, int format, String fname) {
			
		try{
			//On lance les map sur les workers
			WorkerImpl wi = (WorkerImpl) Naming.lookup("//localhost:8000/test");
			 
			//Il faut implémenter les interfaces FileReaderWriter et NetworkReaderWriter	
			wi.runMap(mr,new FileReaderWriter(), new NetworkReaderWriter() );
				
			//On lance aussi le reduce je crois
			mr.reduce(reader, writer);
		} catch (Exception exc)  {
			exc.printStackTrace();
			

		}
	}

	public static <T> void main(String args[]) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		Class classe = Class.forName(args[0]);
		Constructor cst = classe.getConstructor();
		Object mr = cst.newInstance();
		int format = Integer.parseInt(args[1]);
		String fname = args[2];
		startJob((MapReduce) mr,format,fname);
		
	}
}

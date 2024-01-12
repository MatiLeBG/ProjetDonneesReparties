package hdfs;

import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;


public class HdfsClient implements Serializable {
	
	static byte[] buffer = new byte[16384];

	private static void usage() {
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <txt|kv> <file>");
		System.out.println("Usage: java HdfsClient delete <file>");
	}
	
	public static void HdfsDelete(String fname) {
	}
	
	public static void HdfsWrite(int fmt, String fname) {
	}

	public static void HdfsRead(String fname) {
	}

	public static void main(String[] args) {
		// java HdfsClient <read|write> <txt|kv> <file>
		// appel des méthodes précédentes depuis la ligne de commande
		if (args.length > 0) {
		try {
			int nb = Integer.parseInt(args[0]);
			Socket csock = new Socket("localhost",nb);
			ObjectOutputStream oos = new ObjectOutputStream (
			csock.getOutputStream());
			System.out.println(args[0]);
			
			oos.writeObject(nb);
			csock.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	}
}

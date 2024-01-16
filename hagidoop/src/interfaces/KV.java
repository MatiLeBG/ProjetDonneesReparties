package interfaces;

import java.io.Serializable;

public class KV implements Serializable, Cloneable {

	private static final long serialVersionUID = 1L;

	public static final String SEPARATOR = "<->";

	public String k;
	public String v;

	public KV() {
	}

	public KV(String k, String v) {
		super();
		this.k = k;
		this.v = v;
	}

	public String toString() {
		return "KV [k=" + k + ", v=" + v + "]";
	}

	public static KV fromString(String s) {
		if (!(s.equals(""))) {
			KV kv = new KV();
			int startIndex = s.indexOf("k=") + 2;
			int endIndex = s.indexOf(", v=");
			kv.k = s.substring(startIndex, endIndex);
			startIndex = s.indexOf("v=") + 2;
			endIndex = s.indexOf("]");
			kv.v = s.substring(startIndex, endIndex);
			return kv;
		}
		return new KV("", "");
	}

	public Object clone() {
		try {
			return super.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
			return null;
		}
	}

}

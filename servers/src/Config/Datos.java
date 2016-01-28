package Config;

import java.io.Serializable;
import java.util.LinkedList;

import UtilsLib.GFunctions;

public class Datos implements Serializable{
	
	private static Datos instance = null;
	private String LOGFILE = "DBFILE.db";
	private ArrayDB database;
	int curUpdates = 0;
	LinkedList<ArrayDB> pendingUpdates = new LinkedList<ArrayDB>(); 
	
	private Datos () {
		this.database = new ArrayDB();
		this.saveToDBLog(); // --> Primer save
	}
	
	public static Datos getInstance() {
		if ( instance == null ) {
			instance = new Datos();
		}
		return instance;
	}
	
	public void update(int pos, int value) {
		this.database.updateValue(pos, value);
//		curUpdates++;
//		this.pendingUpdates.add(this.database.cloneDB());
	}
	
	public int read(int pos) {
		return this.database.get(pos);
	}
	public int getCurrentUpdates() {
		return curUpdates;
	}
	public int getValue(int pos) {
		return this.database.get(pos);
	}
	public int[] getPendingArray() {
		if (!this.pendingUpdates.isEmpty() ) {
			ArrayDB tmp = pendingUpdates.getFirst();
			return tmp.getDataArray();
		}
		curUpdates = 0;
		return null;
	}
	
	
	public void saveToDBLog() {
		String toSave = "";
		for(int i=0;i<database.size();i++) {
			toSave+= database.get(i)+";";
		}
		toSave = toSave.substring(0,toSave.length()-1); 
		GFunctions.writeToLog(toSave, LOGFILE);
	}

	public ArrayDB getDBArray() {
		return this.database;
	}
	public int size() {
		return this.database.size();
	}
	
	
	
	
	
	class ArrayDB implements Serializable{
		int[] data = new int[50];
		int size = data.length;
		
		public ArrayDB() {
			this.size = data.length;
			for (int i=0; i<this.size; i++) data[i] = 0;	
		}
		public ArrayDB(int[] array) {
			this.size = array.length;
			for (int i=0;i<size; i++) {
				data[i]=array[i];
			}
		}
		public void updateValue(int pos, int value) {
			this.data[pos] = value;
		}
		public int size() {
			return this.size;
		}
		public int get(int pos) {
			return this.data[pos];
		}
		public ArrayDB cloneDB() {
			ArrayDB cloned = null;
			try {
				cloned = (ArrayDB) this.clone();
			} catch (Exception e) {
				cloned = new ArrayDB(this.data);
			}
			return cloned;
		}
		public int[] getDataArray() {
			return this.data;
		}
	}
	
	
}

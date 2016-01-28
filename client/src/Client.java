import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

import Network.Frame;
import UtilsLib.GFunctions;

public class Client {

	public static final String TRANSTYPE_WRITABLE = "CLIENT-WRITE";
	public static final String TRANSTYPE_READABLE = "CLIENT-READ";
	public static final String TRANSTYPE_MALFORMED = "MALFORMED";
	
	private ArrayList<Node> updaters;
	private ArrayList<Node> readersin;
	private ArrayList<Node> readersout;
	private ArrayList<String> loadedTransactions;
	private int actualTransaction;
	private String transactionsFile;
	
	
	public Client(String transFile) {
		this.transactionsFile = transFile;
	}
	
	public Client(String transFile,ArrayList<Node> upd, ArrayList<Node> readin, ArrayList<Node> readout) {
		this.transactionsFile = transFile;
		this.updaters = upd;
		this.readersin = readin;
		this.readersout = readout;
	}
	
	public void printFile() {
		if ( this.transactionsFile != null ) {
			File f = new File(this.transactionsFile);
			if ( f.exists() && f.isFile() ) {
				try {
					FileReader fr = new FileReader(this.transactionsFile);
					BufferedReader in = new BufferedReader(fr);
					String line = "";
					System.out.println("--- Transactions File");
					int nLine = 1;
					try {
						while ((line = in.readLine()) != null ) {
							System.out.println(nLine+": "+line);
							nLine++;
						}
						in.close();
						
					} catch (IOException e) {
						e.printStackTrace();
					}
					System.out.println("--- End of file");
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public boolean loadTransactionsFile() {
		this.loadedTransactions = new ArrayList<String>();
		this.actualTransaction = 0;
		if ( this.transactionsFile != null ) {
			File f = new File(this.transactionsFile);
			if ( f.exists() && f.isFile() ) {
				try {
					FileReader fr = new FileReader(this.transactionsFile);
					BufferedReader in = new BufferedReader(fr);
					String line = "";
					try {
						while ((line = in.readLine()) != null ) {
							// Seria bueno meter aquí un checker para ver si la trama tiene formato correcto.
							// Mediante REGEX como hace el servidor, asi se evitaria enviar mensajes que el servidor descartara
							// Si todas las tramas estan ok se cargan las transacciones, 
							// Sino, se informa al usuario y se crea un arraylist vacio para evitar que se envien
							this.loadedTransactions.add(line);
						}
						in.close();
						return true;
					} catch (IOException e) {
						e.printStackTrace();
					}
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
			}
		}
		return false;
	}
	
	public String getNextTransaction() {
		if ( actualTransaction < this.loadedTransactions.size() ) {
			String tmp = this.loadedTransactions.get(actualTransaction);
			actualTransaction++;
			return tmp;
		} else {
			return null;
		}
	}
	
	public void printNodes() {
		
		System.out.println("---------------------------------------------");
		System.out.println("-               NODES LOADED                -");
		System.out.println("---------------------------------------------");
		System.out.println(String.format("%-15s%-20s%-10s","Layer","IP/Hostname","Port"));
		System.out.println("---------------------------------------------");
		for (Node nod : updaters) {
			System.out.println(String.format("%-15s%-20s%-10s","Core",nod.getHost(),nod.getPort()));	
		}
		for (Node nod : readersin) {
			System.out.println(String.format("%-15s%-20s%-10s","Read 1",nod.getHost(),nod.getPort()));	
		}
		for (Node nod : updaters) {
			System.out.println(String.format("%-15s%-20s%-10s","Read 2",nod.getHost(),nod.getPort()));	
		}
		System.out.println("---------------------------------------------");
	}
	
	public String sendNextTransaction(String transaction) {
		String response = null;
		if ( transaction != null ) {
			// Obtenemos el tipo de transaction que vamos a ejecutar (READ o WRITE)
			String transtype = this.getTypeOfTransaction(transaction);
			// Obtenemos un nodo dependiendo del tipo de transaction que vamos a ejecutar
			Node selectedServer = this.selectNode(transaction);
			String preparedTrans = this.prepareTransactionToSend(transaction);
			if (selectedServer != null ) {
				try {
					System.out.println("TYPE: "+transtype+" - TRANS: "+transaction+" NODE["+selectedServer.getHost()+":"+selectedServer.getPort()+"]");
					Socket connServer = new Socket(selectedServer.getHost(),selectedServer.getPort());
					ObjectOutputStream oStream = new ObjectOutputStream(connServer.getOutputStream());
					ObjectInputStream iStream = new ObjectInputStream(connServer.getInputStream());
					// Sending frame
					Frame frameTX = new Frame(null,null,transtype,0,preparedTrans,0);
					oStream.writeObject(frameTX);
					
					//Getting response
					Frame frameRX = (Frame) iStream.readObject();
					response = frameRX.frame_message;
					
					oStream.close();
					iStream.close();
					connServer.close();
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return response;
	}
	
	private String getTypeOfTransaction(String transact) {
		// Realizar una comprobacion mas exhaustiva de la transaction --> TODO
		if ( transact.indexOf("w") != -1 ) {
			return TRANSTYPE_WRITABLE;
		} 
		return TRANSTYPE_READABLE;
	}
	
	private Node selectNode(String transaction) {
		if ( transaction.indexOf("w") != -1 ) {
			return updaters.get(GFunctions.getRandomFromZero(updaters.size()));
		} else {
			String[] transSplitted = transaction.split(",");
			transSplitted[0] = transSplitted[0].replace("b", "").replace("<", "").replace(">", "").trim();
			try {
				int layer = Integer.parseInt(transSplitted[0]);
				switch(layer) {
					case 0:
						return updaters.get(GFunctions.getRandomFromZero(updaters.size()));
					case 1:
						return readersin.get(GFunctions.getRandomFromZero(readersin.size()));
					case 2:
						return readersout.get(GFunctions.getRandomFromZero(readersout.size()));
					default:
						return readersout.get(GFunctions.getRandomFromZero(readersout.size()));
				}
			} catch (Exception e) {
				System.out.println("MALFORMED TRANSACTION ["+transaction+"]");
				e.printStackTrace();
				return null;
			}
		}
	}
	
	private String prepareTransactionToSend(String transaction) {
		int firstComma = transaction.indexOf(",");
		int lastComma = transaction.lastIndexOf(",");
		String preparedTransaction = transaction.substring(firstComma+1,lastComma);
//		System.out.println("PREPARED TRANS: "+preparedTransaction);
		return preparedTransaction; 
	}
	
}

package Network;


import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import Config.Datos;
import Distributed.RAMutex;
import UtilsLib.GFunctions;

public class ServerCore {

	private int server_port;
	private ServerSocket server;
	private ArrayList<Node> nodeCoreList;
	private ArrayList<Node> nodeReadList;
	private int nodeReadListSize;
	private Datos database = Datos.getInstance();
	private RAMutex distrib_mutex;
	private Lock mutex_writers;
	private Lock mutex_readers;
	private Lock mutex_CSRequest;
	private Lock mutex_pendingUpdates;
	private int operationsCountTemp;
	private int tstamp;
	private int readers;
	private int writers;
	private Hashtable<String,String> pendingSinc;
	private Node localnode;
	private String LOG_OPERATIONS = "DBFILE.db";
	
	
	private static final String TRANSTYPE_CORE = "CORE";
	private static final String TRANSTYPE_CLIENT_READ = "CLIENT-READ";
	private static final String TRANSTYPE_CLIENT_WRITE= "CLIENT-WRITE";
	private static final String TRANSRESP_CORRECT = "OK";
	private static final String TRANSRESP_INCORRECT = "KO";
	
	
	private Pattern pRead = Pattern.compile("^[r]\\(\\d{1,2}\\)");
	private Pattern pWrite= Pattern.compile("^[w]\\(\\d{1,2}\\:\\d{1,2}\\)");
	
	
	
	public ServerCore(Node localnode, RAMutex mutex, ArrayList<Node> nodesCore, ArrayList<Node> readersIn) {
		this.localnode = localnode;
		this.server_port = localnode.getPort();
		this.distrib_mutex = mutex;
		this.readers = 0;
		this.writers = 0;
		this.mutex_readers = new ReentrantLock();
		this.mutex_writers = new ReentrantLock();
		this.mutex_CSRequest = new ReentrantLock();
		this.mutex_pendingUpdates = new ReentrantLock();
		this.nodeCoreList = nodesCore;
		this.nodeReadList = this.getMyLowLayerNodes(readersIn);
		this.nodeReadListSize = this.nodeReadList.size();
		System.out.println("Loaded numberOfNodes "+nodeReadListSize+":"+nodeReadList.size());
		this.operationsCountTemp = 0;
		this.pendingSinc = new Hashtable<String,String>();
	}
	
	public ArrayList<Node> getMyLowLayerNodes(ArrayList<Node> allLowNodes) {
		String[] nodesName = localnode.getLowlayerNodes();
		ArrayList<Node> lowNodes = new ArrayList<Node>();
		for (int i=0; i<allLowNodes.size(); i++) {
			for(int j=0; j<nodesName.length; j++) {
//				System.out.println("Checking LowLayerNode:"+allLowNodes.get(i).getName()+" VS "+nodesName[j]);
				if (nodesName[j].equalsIgnoreCase(allLowNodes.get(i).getName())) {
					lowNodes.add(allLowNodes.get(i));
					break;
				}
			}
		}
		return lowNodes;
	}
	
	public void startServer() {
		try {
			
			this.server = new ServerSocket(this.server_port);
			GFunctions.writeToScreen("INFO: Started server on port: "+localnode.getPort());
			while (true) {
				(new ClientCoreHandler(server.accept())).start();
			}
		} catch (Exception e) {
			GFunctions.writeToScreen("ERROR: Can't start server on port: "+this.server_port);
			GFunctions.writeToScreen("ERROR: Application will exit now");
			System.exit(-1);
		}
	}
	
	class ClientCoreHandler extends Thread {
		private Socket client;
		private ObjectOutputStream oStream = null;
		private ObjectInputStream iStream = null;
		
		public ClientCoreHandler(Socket clientSocket) {
			this.client = clientSocket;
		}
		
		@Override
		public void run() {
			this.handleClient();
		}
		
		private void handleClient() {
			try {
				System.out.println("NEWC: New connection from "+this.client.getRemoteSocketAddress());
				iStream = new ObjectInputStream(this.client.getInputStream());
				System.out.println("NEWC: Opened InputStream");
				oStream = new ObjectOutputStream(this.client.getOutputStream());
				System.out.println("NEWC: Opened OutputStream");
				Frame frameRX = (Frame) iStream.readObject();
				if ( frameRX.frame_type != null ) {
					switch(frameRX.frame_type) {
						case TRANSTYPE_CORE:
							this.handleCoreUpdate(frameRX);
						break;
						case TRANSTYPE_CLIENT_WRITE:
							this.handleClientUpdate(frameRX);
						break;
						case TRANSTYPE_CLIENT_READ:
							this.handleClientRead(frameRX);
						break;
					}
				}
						
				iStream.close();
				oStream.close();
				client.close();
			} catch (Exception e){
				e.printStackTrace();
			}
		}
		
		private void handleClientRead(Frame frm) throws IOException {
			System.out.println("RCVD: Frame received ["+frm.frame_message+"]");
			String[][] ops = this.extractOperations(0, frm.frame_message);
			String response = "";
			String typeresp = "";
			
			if ( ops != null ) {
	
//	START READERS-WRITERS
				mutex_readers.lock();
					readers++;
					if (readers == 1) {
						mutex_writers.lock();
					}
				mutex_readers.unlock();

				response = this.readValues(ops[1]);
				typeresp = TRANSRESP_CORRECT;

  				mutex_readers.lock();
  					readers--;
  					if (readers == 0) {
  						mutex_writers.unlock();
  					}
  				mutex_readers.unlock();
// 	END READERS WRITERS

			} else {
				response = "Malformed transaction";
				typeresp = TRANSRESP_INCORRECT;
			}
			
			System.out.println("SEND: Sending response ["+response+"] to client");
			Frame frameTX = new Frame(null,null,typeresp,0,response,0);
			this.oStream.writeObject(frameTX);
			// Aunque es un read , realiza tambien la comprobacion y aumenta el numero de operaciones
			if ( nodeReadListSize > 0 ) { this.addUpdatesToPending(ops[0]); }
			
		}
		
		private void handleCoreUpdate(Frame frm) {
			// Aquí no comprobamos nada puesto que si estamos gestionando esta conexion
			// el nodo que la ha enviado ya se ha encargado de comprobar que el valor sea correcto
			GFunctions.writeToScreen("SYNC: Update frame received from core with ["+frm.frame_message+"]");			
			int numWrites = StringUtils.countMatches(frm.frame_message, "w"); 
			String[][] ops = this.extractOperations(numWrites, frm.frame_message);
			if ( ops != null ) { 
				mutex_writers.lock();
					this.writeValues(ops[0]);
					// Esta parte tambien, asi nos aseguramos de que las operaciones de actualizacion se escriben
					// en el orden correcto y no que pueda avanzarse un thread a otro y por lo tanto escribir
					// en la lista antes una actualizacion que va despues
					if ( nodeReadListSize > 0 ) { 
						this.addUpdatesToPending(ops[0]); 
					}
				mutex_writers.unlock();
			}
		}
		
		private void handleClientUpdate(Frame frm) throws IOException {
			String response = "";
			String typeresp = "";
			String transact = frm.frame_message;
			GFunctions.writeToScreen("RCVD: Update frame received from core with ["+frm.frame_message+"]");
			int numWrites = StringUtils.countMatches(transact, "w"); 
			String[][] ops = extractOperations(numWrites, transact);
			if ( ops != null ) {
				if (  numWrites > 0 ) {
					mutex_CSRequest.lock();
						distrib_mutex.requestCS(); // Request Distributed Critical Section
							mutex_writers.lock();
								//Debido a que es EAGER, enviamos la trama a los otros node CORE
								this.sendToCoreNodes(frm);
								// Una vez enviada procesamos la trama.
								this.writeValues(ops[0]);
								response = this.readValues(ops[1]);
								if ( nodeReadListSize > 0 ) this.addUpdatesToPending(ops[0]);
							mutex_writers.unlock();
						distrib_mutex.releaseCS(); // Release Distributed Critical Section
					mutex_CSRequest.unlock();
					typeresp = TRANSRESP_CORRECT;
				} else {
					response = "Malformed";
					typeresp = TRANSRESP_INCORRECT;
				}
			} else {
				response = "Malformed";
				typeresp = TRANSRESP_INCORRECT;
			}
			System.out.println("SEND: Sending response ["+response+"] to client");
			Frame frameTX = new Frame(null,null,typeresp,0,response,0);
			this.oStream.writeObject(frameTX);
		}
		private void addUpdatesToPending(String[] ops) {
			mutex_pendingUpdates.lock();
			if (ops != null && ops.length > 0) {
				for(int i=0;i<ops.length;i++) { 
					String[] data = ops[i].split(":");
					pendingSinc.put(data[0], data[1]);
				}
			} 
			operationsCountTemp++;
			if (operationsCountTemp >= 10) this.updateLowLayer();
			mutex_pendingUpdates.unlock();
		}
		
		private void updateLowLayer() {
//			System.out.println("Entre en el updateLowLayer");
			if ( !pendingSinc.isEmpty() ) {
				String prepared = ""; 
				Set set = pendingSinc.entrySet();
			    Iterator it = set.iterator();
			    while ( it.hasNext() ) {
			    	@SuppressWarnings("rawtypes")
			    	Map.Entry entry = (Map.Entry) it.next();
			    	prepared += entry.getKey()+":"+entry.getValue()+",";
			    }
			    pendingSinc = new Hashtable<String,String>(); // Clean updated values
			    if ( prepared.length() > 0 ) { prepared = prepared.substring(0,prepared.length()-1); } // Remove last ,
				Frame frameTX = new Frame(null,null,TRANSTYPE_CORE,0,prepared,0);
				for (Node nod: nodeReadList) {
					try {
						GFunctions.writeToScreen("SYNC: Update frame to "+nod.toString()+" with data:["+frameTX.frame_message+"]");
//						System.out.println("Enviando trama a nodo lowlayer: "+nod.toString()+"");
						Socket conn = new Socket(nod.getHost(),nod.getPort());
//						System.out.println("Abierta  conexion");
						ObjectOutputStream oStream = new ObjectOutputStream(conn.getOutputStream());
						ObjectInputStream iStream = new ObjectInputStream(conn.getInputStream());
//						System.out.println("Abierto outputStream "+nod.toString()+"");
						oStream.writeObject(frameTX);
						
//						System.out.println("Escrita la trama: "+nod.toString()+"");
						oStream.close();
						conn.close();
					} catch (UnknownHostException e) {
						GFunctions.writeToScreen("ERROR: Unknow host ");
//						e.printStackTrace();
					} catch (IOException e) {
						GFunctions.writeToScreen("ERROR: Connection established to ["+nod.toString()+"], but can't send frame");
//						e.printStackTrace();
					}
				}
				operationsCountTemp = 0;
			}
		}
		
		private String readValues(String[] ops) {
			String response = "";
			int readOps = ops.length;
			for(int i=0; i<readOps; i++) {
				System.out.println("\tOPRD: Reading position "+ops[i]);
				int position = Integer.parseInt(ops[i]);
				response += position+":"+database.getValue(position)+",";
			}
			if ( response.endsWith(",") ) { response = response.substring(0,response.length() - 1); }
			return response;
		}
		
		private void writeValues(String[] ops) {
			if ( ops != null ) {
				int writeOps = ops.length;
				for (int i=0; i<writeOps;i++) {
					String[] writeOP = ops[i].split(":");
					int pos = Integer.parseInt(writeOP[0]);
					int val = Integer.parseInt(writeOP[1]);
					System.out.println("\tOPWR: Writting position "+pos+" with value "+val);
					database.update(pos, val);
				}
				database.saveToDBLog(); // Guardamos en el log los cambios
			} else {
				GFunctions.writeToScreen("WARN: No write operations in frame");
			}
		}
		
		private void sendToCoreNodes(Frame frm) throws UnknownHostException, IOException {
			Frame frameTX = new Frame(null,null,TRANSTYPE_CORE,0,frm.frame_message,0);
			
			for (Node core : nodeCoreList) {
				GFunctions.writeToScreen("SYNC: Sending update to "+core.toString()+" ["+frm.frame_message+"]");
				Socket coreSocket = new Socket(core.getHost(),core.getPort() );
				ObjectOutputStream oStream = new ObjectOutputStream(coreSocket.getOutputStream());
				ObjectInputStream iStream = new ObjectInputStream(coreSocket.getInputStream());
				oStream.writeObject(frameTX);
				oStream.close();
				coreSocket.close();
			}
		}
		
		private String[][] extractOperations(int numWrites, String transaction) {
			String[] ops = transaction.split(",");
			int opSize = ops.length;
			int writer = 0;
			int reader = 0;
			
			
			String[][] operations = new String[2][1];
			operations[0] = new String[numWrites];
//			System.out.println("NUMWRITES: "+numWrites+" NUMREADS: "+(opSize-numWrites));
			operations[1] = new String[(opSize-numWrites)];
			
			for (int i=0; i<opSize; i++) {
				if ( ops[i] != null ) {
					Matcher mRead = pRead.matcher(ops[i]);
					Matcher mWrite= pWrite.matcher(ops[i]);
					
					if ( mRead.matches() ) {
						operations[1][reader] = ops[i].replace("r", "").replace("(","").replace(")", "").trim(); 
						reader++;
					} else if ( mWrite.matches() ) {
						operations[0][writer] = ops[i].replace("w", "").replace("(","").replace(")", "").trim();
						writer++;
					} else {
						// Malformed transaction!
						return null;
					}
				} else {
					// Malformed 
					return null;
				}
			}
			return operations;
		}
		
	

	}
}

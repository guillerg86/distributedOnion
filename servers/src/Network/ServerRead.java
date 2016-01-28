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
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import Config.Datos;
import UtilsLib.GFunctions;

public class ServerRead {

	private Node localnode;
	private ArrayList<Node> myLayerNodes;
	private ArrayList<Node> lowLayerNodes;
	private Datos database = Datos.getInstance();
	private Hashtable<String,String> pendingSinc;
	
	private ServerSocket server;
	
	private static final String TRANSTYPE_CORE = "CORE";
	private static final String TRANSTYPE_CLIENT_READ = "CLIENT-READ";
	private static final String TRANSRESP_CORRECT = "OK";
	private static final String TRANSRESP_INCORRECT = "KO";
	
	private Pattern pRead = Pattern.compile("^[r]\\(\\d{1,2}\\)");
	private Pattern pWrite= Pattern.compile("\\d{1,2}\\:\\d{1,2}");
	
	public ServerRead(Node localnode, ArrayList<Node> myLayerNodes, ArrayList<Node> lowLayerNodes) {
		this.localnode = localnode;
		this.myLayerNodes = myLayerNodes;
		this.lowLayerNodes = this.getMyLowLayerNodes(lowLayerNodes);
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
			this.server = new ServerSocket(this.localnode.getPort());
			GFunctions.writeToScreen("INFO: Started server on port: "+localnode.getPort());
			if ( lowLayerNodes.size() > 0 ) {
				TimerNotifier notifierToLowLayer = new TimerNotifier();
				notifierToLowLayer.start();
			} else {
				System.out.println("INFO: 0 Lownodes to notify");
			}
			while (true) {
				ClientReadinHandler cliHandler = new ClientReadinHandler(server.accept());
				cliHandler.start();
			}
		} catch (Exception e) {
			GFunctions.writeToScreen("ERROR: Can't start server on port: "+this.localnode.getPort());
			GFunctions.writeToScreen("ERROR: Application will exit now");
			System.exit(-1);
		}
	}
	
	
/////////////////////////////	
	
	class ClientReadinHandler extends Thread{
		private Socket client;
		private ObjectOutputStream oStream = null;
		private ObjectInputStream iStream = null;
		
		public ClientReadinHandler(Socket client) {
			this.client = client;
		}
		
		@Override
		public void run() {
			this.handleClient();
		}
		
		private void handleClient() {
			try {
				iStream = new ObjectInputStream(client.getInputStream());
				oStream = new ObjectOutputStream(client.getOutputStream());
				
				Frame frameRX = (Frame) iStream.readObject();
				if ( frameRX.frame_type != null ) {
					switch(frameRX.frame_type) {
						case TRANSTYPE_CORE:
							this.handleCoreUpdate(frameRX);
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
		
		private void handleCoreUpdate(Frame frm) {
			// Aquí no comprobamos nada puesto que si estamos gestionando esta conexion
			// el nodo que la ha enviado ya se ha encargado de comprobar que el valor sea correcto
//			mutex_writers.lock();
			GFunctions.writeToScreen("SYNC: Update frame received from core with ["+frm.frame_message+"]");
			this.writeValues(frm.frame_message.split(","));
			// Revisamos si somos un nodo que ha de notificar a la capa inferior, añadimos y comprobamos cuantas operaciones llevamos
			if ( lowLayerNodes.size() > 0 ) { this.addUpdatesToPending(frm.frame_message.split(",")); }
//			mutex_writers.unlock();
		}
		
		private void handleClientRead(Frame frm) throws IOException {
			System.out.println("RCVD: Frame received ["+frm.frame_message+"]");
			String[] ops = this.extractReadOperations(frm.frame_message);
			String response = "";
			String typeresp = "";
			if ( ops != null ) {
				response = this.readValues(ops);
				typeresp = TRANSRESP_CORRECT;
			} else {
				response = "Malformed transaction";
				typeresp = TRANSRESP_INCORRECT;
			}
			System.out.println("SEND: Sending response ["+response+"] to client");
			Frame frameTX = new Frame(null,null,typeresp,0,response,0);
			this.oStream.writeObject(frameTX);
			// Aunque es un read , realiza tambien la comprobacion y aumenta el numero de operaciones
		}
		
		private void addUpdatesToPending(String[] ops) {
			if (ops != null && ops.length > 0) {
				for (int i=0; i<ops.length; i++) {
					String[] data = ops[i].split(":");
					pendingSinc.put(data[0], data[1]);
				}
			}
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
				database.saveToDBLog();
			} else {
				GFunctions.writeToScreen("WARN: No write operations in frame");
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
		
		private String[] extractReadOperations(String transaction) {
			String[] ops = transaction.split(",");
			int opSize = ops.length;
		
			for (int i=0; i<opSize; i++) {
				if ( ops[i] != null ) {
					Matcher mRead = pRead.matcher(ops[i]);
					
					if ( mRead.matches() ) {
						ops[i] = ops[i].replace("r", "").replace("(","").replace(")", "").trim(); 
					} else {
						// Malformed transaction!
						return null;
					}
				} else {
					// Malformed 
					return null;
				}
			}
			return ops;
		}
	}
	
///////////////
	
	class TimerNotifier extends Thread {
		@Override
		public void run() {
			System.out.println("INFO: Starting notifier Thread for "+lowLayerNodes.size()+" nodes in low layer");
			while (true) {
				GFunctions.wait(10000);
				if ( !pendingSinc.isEmpty() ) {
					String transactions = ""; 
					Set set = pendingSinc.entrySet();
				    Iterator it = set.iterator();
				    while ( it.hasNext() ) {
				    	@SuppressWarnings("rawtypes")
				    	Map.Entry entry = (Map.Entry) it.next();
				    	transactions += entry.getKey()+":"+entry.getValue()+",";
				    }
				    pendingSinc = new Hashtable<String,String>(); // Clean updated values
				    transactions = transactions.substring(0,transactions.length()-1); // Remove last ,
					
					Frame frameTX = new Frame(null,null,TRANSTYPE_CORE,0,transactions,0);
					for (Node nod: lowLayerNodes) {
						try {
							GFunctions.writeToScreen("SYNC: Update frame to "+nod.toString()+" with data:["+frameTX.frame_message+"]");
							Socket conn = new Socket(nod.getHost(),nod.getPort());
							ObjectOutputStream oStream = new ObjectOutputStream(conn.getOutputStream());
							ObjectInputStream iStream = new ObjectInputStream(conn.getInputStream());
							oStream.writeObject(frameTX);
							
							oStream.close();
							iStream.close();
							conn.close();
						} catch (UnknownHostException e) {
							GFunctions.writeToScreen("ERROR: Unknow host ");
//							e.printStackTrace();
						} catch (IOException e) {
							GFunctions.writeToScreen("ERROR: Connection established to ["+nod.toString()+"], but can't send frame");
//							e.printStackTrace();
						}
					}
				}
			}
		}
	
	}
}

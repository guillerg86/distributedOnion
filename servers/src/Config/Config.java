package Config;



import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Properties;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import Network.Node;
import UtilsLib.GFunctions;

public class Config {

	public static final String CONFIGFILE = "config.properties";
	public static Config instance = null;
	
	
	private String logFile;
	private String webService; 
	
	private ArrayList<Node> cores;
	private ArrayList<Node> readers_out;
	private ArrayList<Node> readers_in;
	private Node localnode;
	
	private Config() {
		this.cores = new ArrayList<Node>();
		this.readers_out = new ArrayList<Node>();
		this.readers_in = new ArrayList<Node>();
	}
	public static Config getInstance() {
		if ( instance == null) {
			instance = new Config();
		}
		return instance;
	}
	
	public boolean loadConfig() {
		Properties props = this.getProperties();
		if ( props != null ) {
			try {
				String node_name = props.getProperty("nodename");
				this.logFile = props.getProperty("apliclog");
				this.webService = props.getProperty("configws");
				GFunctions.createLogFile(node_name);
				
				
				
				return this.loadConfigFromWS(node_name);
			} catch (Exception e) {
				return false;
			}
		}
		return false;
	}
	
	public Properties getProperties() {
		Properties properties;
		try{ 
			FileReader reader = new FileReader(Config.CONFIGFILE);
			properties = new Properties();
			properties.load(reader);
		} catch (Exception e) {
			properties = null;
		}
		return properties;
	}	
	
	private boolean loadConfigFromWS(String myname) {
		boolean loaded = true;
		try {
			URL url = new URL(this.webService);	
			HttpURLConnection connection =(HttpURLConnection) url.openConnection();
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			String line = "";
			String json = "";
			
			while ((line = reader.readLine()) != null){
				json +=line;
			}
			
			Gson gson = new GsonBuilder().create();
			JsonObject object = gson.fromJson(json, JsonObject.class);
			JsonArray data = object.get("data").getAsJsonArray();
			
	    	for (JsonElement element: data){
	    		
	    		String name = element.getAsJsonObject().get("sv_name").getAsString().toLowerCase();
    			String host = "";
    			String port = "";
    			String role = "";
    			String mode = "";
    			String mtxp = "";
	    		String lowl = "";
    			
    			if ( name != null ) {
    				// Si somos nosotros, no nos guardamos los datos
    				host = element.getAsJsonObject().get("sv_host").getAsString().toLowerCase();
        			port = element.getAsJsonObject().get("sv_port").getAsString();
        			role = element.getAsJsonObject().get("sv_role").getAsString();
        			mtxp = element.getAsJsonObject().get("sv_mutexport").getAsString();
        			mode = element.getAsJsonObject().get("sv_prim").getAsString();
        			lowl = element.getAsJsonObject().get("lowlayer").getAsString().toLowerCase();
        			
        			try {
	        			if ( host != null && port != null && mode != null ) {
	            			switch(role) {
	            				case "core":
	            					if ( !name.equalsIgnoreCase(myname) ) {
	                					this.cores.add(new Node(name,host,Integer.parseInt(port),Integer.parseInt(mtxp),lowl,role));
	                					GFunctions.writeToLog("Cargado el nodo: "+name+" - "+host+" - "+port+" - "+mode+" ", this.logFile ,GFunctions.MSG_TYPE_INFO);
	                				} else {
	                					this.localnode = new Node(name,host,Integer.parseInt(port),Integer.parseInt(mtxp),lowl,role);
	                				}
	            				break;
	            				case "readin":
	            					boolean tmp = false; if ( mode.equals("true") ) { tmp = true; }
	            					if ( !name.equalsIgnoreCase(myname) ) {
	            						this.readers_in.add(new Node(name, host, Integer.parseInt(port),lowl,role));
	            					} else {
	            						this.localnode = new Node(name, host, Integer.parseInt(port),lowl,role);
	            					}
	            					GFunctions.writeToLog("Cargado el nodo: "+name+" - "+host+" - "+port+" - "+mode+" ",this.logFile ,GFunctions.MSG_TYPE_INFO);
	            				break;
	            				case "readout":
	            					boolean tmp2 = false; if ( mode.equals("true") ) { tmp2 = true; }
	            					if ( !name.equalsIgnoreCase(myname) ) {
	            						this.readers_out.add(new Node(name, host, Integer.parseInt(port),lowl,role));
	            					} else {
	            						this.localnode = new Node(name, host, Integer.parseInt(port),lowl,role);
	            					}
	              					GFunctions.writeToLog("Cargado el nodo: "+name+" - "+host+" - "+port+" - "+mode+" ",this.logFile ,GFunctions.MSG_TYPE_INFO);
	            				break;
	            				default:
	            					GFunctions.writeToLog("Error al cargar el nodo: "+name+" - "+host+" - "+port+" - "+mode+" ", this.logFile, GFunctions.MSG_TYPE_ERROR);
	            				break;
	            			}
	        			}
        			}catch (Exception e) {
    					GFunctions.writeToLog("Error al cargar el nodo "+e.getStackTrace(), this.logFile);
    				}
    			}
	    	}
	    	int nodesLoaded = this.cores.size() + this.readers_in.size() + this.readers_out.size();
	    	if ( nodesLoaded == 0 ) {
	    		GFunctions.writeToLog("No se han cargado nodos", this.logFile , GFunctions.MSG_TYPE_ERROR);
	    		return false;
	    	}
		} catch (Exception e) {
			e.printStackTrace();
			loaded = false;
		}
		return loaded;
	}
	public String getLogFile() {
		return logFile;
	}
	public ArrayList<Node> getNodesCore() {
		return cores;
	}
	public ArrayList<Node> getNodesReaderOut() {
		return readers_out;
	}
	public ArrayList<Node> getNodesReaderIn() {
		return readers_in;
	}
	public Node getLocalNode() {
		return this.localnode;
	}
	
	public void printNodesLoaded() {
		System.out.println("---------------------------------------------");
		System.out.println("-               NODES LOADED                -");
		System.out.println("---------------------------------------------");
		System.out.println(String.format("%-15s%-20s%-10s","Layer","IP/Hostname","Port"));
		System.out.println("---------------------------------------------");
		for (Node nod : cores) {
			System.out.println(String.format("%-15s%-20s%-10s","Core",nod.getHost(),nod.getPort()));	
		}
		for (Node nod : readers_in) {
			System.out.println(String.format("%-15s%-20s%-10s","Read 1",nod.getHost(),nod.getPort()));	
		}
		for (Node nod : readers_out) {
			System.out.println(String.format("%-15s%-20s%-10s","Read 2",nod.getHost(),nod.getPort()));	
		}
		System.out.println("---------------------------------------------");
	}
	
}

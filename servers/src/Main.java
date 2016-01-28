import java.util.ArrayList;

import Config.Config;
import Distributed.RAMutex;
import Network.Node;
import Network.ServerCore;
import Network.ServerRead;
import UtilsLib.GFunctions;

public class Main {
		
	public static void main(String[] args) throws Exception {
		Config cfg = Config.getInstance();
		Node localnode = null;
		RAMutex mutex = null;
		if ( cfg.loadConfig() ) {
			cfg.printNodesLoaded();
			localnode = cfg.getLocalNode();
			GFunctions.writeToScreen("INFO: This node is configured with data ["+localnode.toString()+"]\n");
			switch (localnode.getRole()) {
				case "core":
					mutex = new RAMutex(localnode,cfg.getNodesCore(),true);
					GFunctions.writeToScreen("INFO: Started RAMutex, waitting 10 secs to calibrate");
					GFunctions.wait(10000);
					GFunctions.writeToScreen("INFO: Starting calibration");
					mutex.calibrate();
					GFunctions.writeToScreen("INFO: Calibration end\n\n");
					
					ServerCore coreServer = new ServerCore(localnode,mutex,cfg.getNodesCore(),cfg.getNodesReaderIn());	
					coreServer.startServer();
				break;
				case "readin":
					ServerRead readinServer = new ServerRead(localnode, cfg.getNodesReaderIn(), cfg.getNodesReaderOut());
					readinServer.startServer();
				break;
				case "readout":
					ServerRead readoutServer = new ServerRead(localnode, cfg.getNodesReaderOut(), new ArrayList<Node>() );
					readoutServer.startServer();
				break;
			}
		} else {
			GFunctions.writeToScreen("ERROR: Se ha producido un error al cargar la configuracion");
			GFunctions.writeToScreen("ERROR: Revise el fichero de logs para mayor informacion");
		}
		
		
	}

}

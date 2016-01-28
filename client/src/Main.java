import UtilsLib.GFunctions;

public class Main {
	public static void main(String[] args) {
		Config cfg = Config.getInstance();
		String file = "transactions.txt";
		
		if ( cfg.loadConfig() ) {
			
			Client cl = new Client(file, cfg.getNodesCore(),cfg.getNodesReaderIn(),cfg.getNodesReaderOut());
			System.out.println("PRINTING INFO LOADED\n");
			cl.printNodes();
			cl.printFile();
			System.out.println("/PRINTING INFO LOADED\n\n");
			
			if ( cl.loadTransactionsFile() ) {
				// Ya tenemos en la memoria uno de los ficheros
				String trans = "";
				while( (trans = cl.getNextTransaction()) != null ) {
					String resp = cl.sendNextTransaction(trans);
					System.out.println("Response from server: "+resp);
				}
			}
		} else {
			System.out.println("No se pudo cargar la configuracion");
		}
	}
}

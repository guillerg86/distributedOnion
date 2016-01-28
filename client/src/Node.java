
public class Node {
	
	private String host;
	private int port;
	
	public Node(String h, int p) {
		this.host = h;
		this.port = p;
	}
	
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
}

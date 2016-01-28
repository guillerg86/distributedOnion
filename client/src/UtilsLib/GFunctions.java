package UtilsLib;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Random;

public class GFunctions {
	
	public static final String MSG_TYPE_INFO = "INFO";
	public static final String MSG_TYPE_WARN = "WARN";
	public static final String MSG_TYPE_ERROR = "ERRO";
	
	
	
	public static String cleanNullString(String inputString) {
		String outString = "";
		if ( inputString == null ) return outString; 
		if ( inputString.equals("null") ) return outString;
		if ( inputString.equals("(null)") ) return outString;
		return inputString.trim();
	}

	public static void writeToScreen(String message) {
		System.out.println(message);
	}
	
	public static void writeToLog(String message, String logFile, String msgType) throws Exception {
		DataOutputStream fileLog = new DataOutputStream( new BufferedOutputStream( new FileOutputStream(logFile,true) ) );
		long sysTS = GFunctions.getSystemTimestamp();
		fileLog.writeBytes(sysTS+";"+msgType+";"+message+"\n");
		fileLog.close();
	}
	
	public static void writeToLogWithTimestamp(String message, String logFile) throws Exception{
		DataOutputStream fileLog = new DataOutputStream( new BufferedOutputStream( new FileOutputStream(logFile,true) ) );
		long sysTS = GFunctions.getSystemTimestamp();
		fileLog.writeBytes(sysTS+";"+GFunctions.MSG_TYPE_INFO+";"+message+"\n");
		fileLog.close();		
	}
	
	public static void writeToLog(String message, String logfile) {
		try {
			DataOutputStream fileLog = new DataOutputStream( new BufferedOutputStream( new FileOutputStream(logfile,true) ) );
			fileLog.writeBytes(message+"\n");
			fileLog.close();
		}catch (Exception e) {}
	}
	
	public static void createLogFile(String logFile) throws Exception{
		File file = new File(logFile);
		if ( file.exists() ) {
			File file_back = new File(logFile+"_backup_"+GFunctions.getSystemTimestamp());
				file.renameTo(file_back);
		} else {
			DataOutputStream fileLog = new DataOutputStream( new BufferedOutputStream( new FileOutputStream(logFile) ) );
			long sysTS = GFunctions.getSystemTimestamp();
			fileLog.writeBytes(sysTS+";;"+"First message of log");
		}
	}
	
	public static long getSystemTimestamp() {
		return System.currentTimeMillis();
	}
	public static int getRandomFromZero(int max) {
		Random rand = new Random();
		return rand.nextInt(max);
	}
	public static void wait(int milis) {
		writeToScreen("Esperare "+milis+" ms");
		try {
			Thread.sleep(milis);
		}catch(Exception e){e.printStackTrace();}
		
	}
}

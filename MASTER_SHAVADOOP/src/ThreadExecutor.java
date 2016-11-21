import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;


public class ThreadExecutor extends Thread
{
	public String splitName;
	public String slaveNode;
	public String mode;
	public String SMx;
	public String word;
	public ArrayList<String> singleWords = new ArrayList<String>(); 
	public String wordCount = "";

	// Constructor 1: calls method map in SlaveWork class.
	public ThreadExecutor(String splitName, String slaveNode)
	{
		this.splitName = splitName;
		this.slaveNode = slaveNode;
		this.mode = "SXUMX";
		this.SMx = "";
		this.word = "";
	}

	// Constructor 2: calls methods shuffle and  reduce in SlaveWork class.	
	// Be aware that UMs corresponds to several UM separated by a comma
	public ThreadExecutor(String UMs, String slaveNode, String mode, String SMx, String word)
	{
		this.splitName = UMs;
		this.slaveNode = slaveNode;
		this.mode = mode;
		this.SMx = SMx;
		this.word = word;
	}

	/**
	 * Specific method for ThreadExecutor class. According to the mode (SXUMX or UMXSMX), slaveWork class will execute in a specific manner.
	 */
	public void run()
	{
		try {
			ProcessBuilder pb = new ProcessBuilder("ssh", slaveNode, "java -jar ~/workspace/SLAVE_SHAVADOOP.jar " + this.mode +
																     " " + this.word + " " + this.SMx + " " + this.splitName);
			Process p = pb.start();
			p.waitFor();
			// Buffered Reader object to read from console 
			BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
			if (this.mode.equals("SXUMX")) {
				String line = br.readLine();
				while (line != null) {
					this.singleWords.add(line);
					line = br.readLine();
				}
			}
			else {
				String lineCount = br.readLine();
				if (lineCount != null) {
					this.wordCount = lineCount;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}    

	public ArrayList<String> getSingleWords()
	{
		return this.singleWords;
	}
	
	public String getLineCount() 
	{
		return this.wordCount;
	}
}
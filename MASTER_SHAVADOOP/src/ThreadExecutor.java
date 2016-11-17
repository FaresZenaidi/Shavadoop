import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;


public class ThreadExecutor extends Thread implements Runnable 
{
	public String splitName;
	public String slaveNode;
	public String mode;
	public String SMx;
	public String word;
	public ArrayList<String> singleWords = new ArrayList<String>(); 
	public String wordCount = "";

	// Constructeur 1 - Splitting, Mapping
	public ThreadExecutor(String splitName, String slaveNode)
	{
		this.splitName = splitName;
		this.slaveNode = slaveNode;
		this.mode = "SXUMX";
		this.SMx = "";
		this.word = "";
	}

	// Constructeur 2 - Shuffling, reducing
	public ThreadExecutor(String splitName, String slaveNode, String mode, String SMx, String word)
	{
		this.splitName = splitName;
		this.slaveNode = slaveNode;
		this.mode = mode;
		this.SMx = SMx;
		this.word = word;
	}

	public void run()
	{
		try {

			ProcessBuilder pb = new ProcessBuilder("ssh", slaveNode, "java -jar ~/workspace/SLAVE_SHAVADOOP.jar " + this.mode +
																     " " + this.word + " " + this.SMx + " " + this.splitName);
			Process p = pb.start();
			p.waitFor();
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


	/*
	public void run(){
		try {
			System.out.println("\t Executing SLAVE over: " +slave_node);
			System.out.println("\t " + slave_node + ": Before the command");
			ProcessBuilder pb = new ProcessBuilder("ssh", slave_node, "java -jar ~/workspace/SLAVE_SHAVADOOP.jar");
			long tempsDebut = System.currentTimeMillis();
			Process p = pb.start();
			p.waitFor();
			long tempsFin = System.currentTimeMillis();
			System.out.println("\t After the command, " +slave_node + ": time elapsed = " + (tempsFin - tempsDebut) / 1000 + "s"); 		
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}    
	 */   
}
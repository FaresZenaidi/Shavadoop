import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class Master_work {

	static String fileDir = ("/cal/homes/mzenaidi/workspace/DATA_FILES");


	public static void main(String[] args) throws IOException, InterruptedException
	{	
		String inputFile = "deontologiePoliceNationale.txt";
		String allHosts = "allHosts.txt";
		int maxNbLinesSplit = 50;
		long tempsDebut = System.currentTimeMillis();
		ArrayList<String> slaves = networkDiscovery(new File(fileDir, allHosts)); 
		// ArrayList<String> splits = splitting(new File(fileDir, inputFile));
		ArrayList<String> splits = splitting2(new File(fileDir, inputFile),  maxNbLinesSplit);

		HashMap<String, ArrayList<String>> UMWordsRep = mapping(splits, slaves);
		shuffling(UMWordsRep, slaves);
		long tempsFin = System.currentTimeMillis();
		System.out.println("Total execution time: "  + (tempsFin - tempsDebut) / 1000 + "s"); 
	}


	// Function that reads filename and returns a list of the names of the splits.
	/*
	public static ArrayList<String> splitting2(File inputFile) throws IOException
	{
		System.out.println("******************** SECOND SPLITTING PROCEDURE ********************");
		ArrayList<String> splits = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		String line = br.readLine();
		int indexSplit = 0;
		int maxNbLines = 50;
		int indexNbline = 0;
		splits.add("S" + indexSplit);
		PrintWriter writer = new PrintWriter(new File(fileDir, "S" + indexSplit));
		while (line != null) {
			if (indexNbline < maxNbLines) {
				if (!line.isEmpty()) {
					// writer = new PrintWriter(new File(fileDir, "S" + indexSplit));
					// writer.println(line);
					line = br.readLine();
					indexNbline++;
				}
				else {
					line = br.readLine();
				}
			}
			else {
				indexSplit++;
				PrintWriter writer2 = new PrintWriter(new File(fileDir, "S" + indexSplit));
				splits.add("S" + indexSplit);
				indexNbline = 0;
			}
		}
		return splits;
	}
	 */
	/*
	public static ArrayList<String> splitting(File inputFile) throws IOException 
	{
		System.out.println("******************** SPLITTING PROCEDURE ********************");
		ArrayList<String> splits = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		String line = br.readLine();
		int index = 0;

		while (line != null) {
			// Gestion des lignes vides
			if (!line.isEmpty()) {
				PrintWriter writer = new PrintWriter(new File(fileDir, "S" + index));
				writer.println(line);
				line = br.readLine();
				splits.add("S" + index);
				index++;
				writer.close();
			}
			else {
				line = br.readLine();
			}
		}  
		br.close();
		System.out.println("\t Number of split files: " + splits.size());
		System.out.println("");
		return splits;
	}

	 */

	public static ArrayList<String> splitting2(File inputFile, int maxNbLinesSplit) throws IOException 
	{
		System.out.println("******************** SPLITTING PROCEDURE N2 ********************");
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		String line = br.readLine();
		int nbLines = 0;
		while (line !=  null) {
			if (!line.isEmpty()) {
				nbLines++;
				line = br.readLine();
			}
			else {
				line = br.readLine();
			}
		}
		br.close();

		BufferedReader br2 = new BufferedReader(new FileReader(inputFile));
		ArrayList<String> splits = new ArrayList<String>();
		int counterLines = 0;
		int nbSplits = (int)(Math.ceil((float) nbLines/maxNbLinesSplit));
		for (int i = 0; i < nbSplits; i++) {
			PrintWriter writer2 = new PrintWriter(new File(fileDir, "S" + i));
			String line2 = br2.readLine();
			while (counterLines < maxNbLinesSplit && line2 != null) {
				if (!line2.isEmpty()) {
					System.out.println(line2);
					writer2.println(line2);
					counterLines++;
					line2 = br2.readLine();		
				}
				else {
					line2 = br2.readLine();		
				}
			}
			counterLines = 0;
			writer2.close();
			splits.add("S" + i);
		}
		br2.close();
		System.out.println("\t Number of split files: " + splits.size());
		System.out.println("");
		return splits;
	}
	
	
	// Returns slave nodes
	public static ArrayList<String> networkDiscovery(File allHosts) throws IOException, InterruptedException 
	{
		BufferedReader br = new BufferedReader(new FileReader(allHosts));
		File connectedHosts = new File(fileDir, "slave_nodes");
		PrintWriter writer = new PrintWriter(connectedHosts);
		ArrayList<String> slaves = new ArrayList<String>();
		String remoteHost = br.readLine();
		System.out.println("******************** NETWORK DISCOVERY ********************");

		while (remoteHost != null) {
			String[] cmd = {"ssh","-o StrictHostKeyChecking=no", remoteHost, "echo ok"};
			ProcessBuilder pb = new ProcessBuilder(cmd);
			Process p = pb.start();
			int exitCode = p.waitFor();
			if (exitCode == 0) {
				System.out.println("\t Connection success to: " +remoteHost);
				writer.println(remoteHost);
				slaves.add(remoteHost);
			}			
			remoteHost = br.readLine();
		}
		br.close();
		writer.close();
		System.out.println("\t Number of slaves: " + slaves.size());
		System.out.println("");
		return slaves;
	}

	public static HashMap<String, ArrayList<String>> mapping(ArrayList<String> SFiles, ArrayList<String> slaves) throws IOException, InterruptedException 
	{
		System.out.println("******************** MAPPING PROCEDURE ********************");
		HashMap<String, String> splitsSlavesRep = new HashMap<String, String>();
		ArrayList<ThreadExecutor> threads = new ArrayList<ThreadExecutor>();
		HashMap<String, ArrayList<String>> UMWordsRep = new HashMap<String, ArrayList<String>>();

		int indexDev = 0;
		for (String sFile: SFiles) {
			ThreadExecutor runnable = new ThreadExecutor(sFile, slaves.get(indexDev));
			splitsSlavesRep.put("UM" + sFile.substring(1, sFile.length()), slaves.get(indexDev));
			threads.add(runnable); 
			runnable.start();
			indexDev++;
			if (indexDev >= slaves.size()) {
				indexDev = 0;
			}		
		}
		System.out.println("Dictionary UMx-Slaves");
		System.out.println(splitsSlavesRep);

		for (ThreadExecutor t: threads) {	
			t.join();  // to close the threads
			UMWordsRep.put("UM" + t.splitName.substring(1, t.splitName.length()), t.getSingleWords());
		}

		System.out.println("Dictionary UMx-Words");
		System.out.println(UMWordsRep);
		System.out.println("");
		return UMWordsRep;
	}


	public static void shuffling(HashMap<String, ArrayList<String>> UMWordsRep, ArrayList<String> slaves) throws IOException, InterruptedException 
	{
		System.out.println("******************** SHUFFLING PROCEDURE ********************");
		HashMap<String, HashSet<String>> wordUMsRep = inverseDic(UMWordsRep);
		System.out.println("Dictionary Word-UMs");
		System.out.println(wordUMsRep);
		ArrayList<ThreadExecutor> threads = new ArrayList<ThreadExecutor>();
		HashMap<String, String> RMSlaveRep = new HashMap<String, String>();
		int indexSM = 0;
		int indexSlave = 0;

		for (String word: wordUMsRep.keySet()) {
			ThreadExecutor runnable = new ThreadExecutor(join(wordUMsRep.get(word)), slaves.get(indexSlave), "UMXSMX", "SM" + indexSM, word.toLowerCase());
			RMSlaveRep.put("RM" + indexSM, slaves.get(indexSlave));
			runnable.start();
			threads.add(runnable);
			indexSM++;
			indexSlave++;
			if (indexSlave >= slaves.size()) {
				indexSlave = 0;
			}		
		}
		System.out.println("Dictionary RMx-Slave");
		System.out.println(RMSlaveRep);	
		System.out.println("");
		PrintWriter writerFinal = new PrintWriter(new File(fileDir, "finalResult"));

		System.out.println("******************** REDUCING and ASSEMBLING PROCEDURES ********************");
		HashMap<String, String> wordCountDic = new HashMap<String, String>();
		for (ThreadExecutor t: threads) {	
			t.join();
			if (!t.wordCount.isEmpty()) {
				String[] wordCount = t.wordCount.split(" ");
				wordCountDic.put(wordCount[0], wordCount[1]);
				writerFinal.println(t.wordCount);
			}
		}

		writerFinal.close();

		int topWord = 50;
		System.out.println("WordCount Result dictionary:");
		System.out.println(MapUtil.sortByValue(wordCountDic, topWord));
		System.out.println("");
		System.out.println("Execution finished.");
	}

	public static String join(HashSet<String> list) 
	{
		String link = ",";
		String res = "";
		int counter = 1;
		for (String l: list) {
			if(counter != list.size()) {
				res += l + link;
				counter++;
			}
			else {
				res += l;
			}
		}
		return res;
	}

	public static HashMap<String, HashSet<String>> inverseDic(HashMap<String, ArrayList<String>> initDic) 
	{
		HashMap<String, HashSet<String>> resultDic = new HashMap<String, HashSet<String>>();
		for (String UMx: initDic.keySet()) {
			for (String word: initDic.get(UMx)) {
				if (!resultDic.containsKey(word)) {
					HashSet<String> wordUMx = new HashSet<String>();
					wordUMx.add(UMx);
					resultDic.put(word, wordUMx);
				}
				else {
					resultDic.get(word).add(UMx);
				}
			}
		}
		return resultDic;
	}
}


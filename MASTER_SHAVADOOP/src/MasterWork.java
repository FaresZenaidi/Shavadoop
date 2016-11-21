import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class MasterWork {

	static String fileDir = ("/cal/homes/mzenaidi/workspace/DATA_FILES");
	
	public static void main(String[] args) throws IOException, InterruptedException
	{	
		String inputFile = "forestierMayotte.txt";
		String allHosts = "allHosts.txt";
		String resultFile = "resultFile.txt";
		int maxNbLinesPerSplit = 1;
		int topWords = 10;
		
		long tempsDebut = System.currentTimeMillis();
		/*
		System.out.println("**********************************************************************\n"
				+          "*				          MAP REDUCE FOR WORD COUNT 				 *\n"
				+          "**********************************************************************\n");
		*/
		
		System.out.println("******************* MAP REDUCE FOR WORD COUNT ****************** \n");
		ArrayList<String> slaves = networkDiscovery(new File(fileDir, allHosts));
		
		ArrayList<String> splits = splitting(new File(fileDir, inputFile),  maxNbLinesPerSplit);
		
		HashMap<String, ArrayList<String>> UMWordsRep = mapping(splits, slaves);
		
		ArrayList<ThreadExecutor> threads = shufflingReducing(UMWordsRep, slaves);
		
		HashMap<String, String> wordCountDic = assembling(threads, new File(fileDir, resultFile));
		
		long tempsFin = System.currentTimeMillis();
		System.out.println("******************** RESULT ********************");
		System.out.println("Sorted wordCount result dictionary (top " + topWords + " words)");
		System.out.println(MapUtil.sortByValue(wordCountDic, topWords));	
		System.out.println("Total execution time: "  + (tempsFin - tempsDebut) / 1000 + "s \n"); 

	}

	/**
	 * Function that reads an input file (allHosts.txt) listing computer names of the network and tests the reachability of each one.
	 * These devices are considered as possible slaves to use in our Shavadoop program.
	 * @param input file listing computer names.
	 * @returns list of names of connected devices. 
	 * @throws IOException, InterruptedException
	 */
	public static ArrayList<String> networkDiscovery(File allHosts) throws IOException, InterruptedException 
	{
		System.out.println("******************** NETWORK DISCOVERY ********************");
		BufferedReader br = new BufferedReader(new FileReader(allHosts));
		File connectedHosts = new File(fileDir, "slave_nodes");
		PrintWriter writer = new PrintWriter(connectedHosts);
		ArrayList<String> slaves = new ArrayList<String>();
		String remoteHost = br.readLine();

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
		System.out.println("\t Number of possible slaves: " + slaves.size() + "\n");
		return slaves;
	}
	
	
	/**
	 * Function that reads specified input file and generates split files according to the maximum number of lines per split parameter.
	 * @param inputFile input file on which the word count procedure is performed.
	 * @param maxNbLinesPerSplit maximum number of lines per split file.
	 * @return list of split files names. 
	 * @throws IOException
	 */
	public static ArrayList<String> splitting(File inputFile, int maxNbLinesPerSplit) throws IOException 
	{
		System.out.println("******************** SPLITTING PROCEDURE ********************");
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

		br = new BufferedReader(new FileReader(inputFile));
		ArrayList<String> splits = new ArrayList<String>();
		int counterLines = 0;
		int nbSplits = (int)(Math.ceil((float) nbLines/maxNbLinesPerSplit));
		for (int i = 0; i < nbSplits; i++) {
			PrintWriter writer = new PrintWriter(new File(fileDir, "S" + i));
			line = br.readLine();
			while (counterLines < maxNbLinesPerSplit && line != null) {
				if (!line.isEmpty()) {
					writer.println(line);
					counterLines++;
					line = br.readLine();		
				}
				else {
					line = br.readLine();		
				}
			}
			counterLines = 0;
			writer.close();
			splits.add("S" + i);
		}
		br.close();
		System.out.println("\t Number of split files: " + splits.size() + "\n");
		return splits;
	}

	
	/**
	 * This function does the mapping procedure by calling the map function of SlaveWork class via ThreadExecutor.
	 * @param splits list of split files names.
	 * @param slaves list of reachable hosts.
	 * @return Dictionary of list of words (value) contained in each UM file (key).
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static HashMap<String, ArrayList<String>> mapping(ArrayList<String> splits, ArrayList<String> slaves) throws IOException, InterruptedException 
	{
		System.out.println("******************** MAPPING PROCEDURE ********************");
		HashMap<String, String> splitsSlavesRepartition = new HashMap<String, String>();
		ArrayList<ThreadExecutor> threads = new ArrayList<ThreadExecutor>();
		HashMap<String, ArrayList<String>> UMWordsRep = new HashMap<String, ArrayList<String>>();

		int indexDev = 0;
		for (String split: splits) {
			ThreadExecutor runnable = new ThreadExecutor(split, slaves.get(indexDev));
			splitsSlavesRepartition.put("UM" + split.substring(1), slaves.get(indexDev));
			threads.add(runnable); 
			runnable.start();
			indexDev++;
			if (indexDev >= slaves.size()) {
				indexDev = 0;
			}		
		}
		System.out.println("Dictionary UMx-Slaves");
		System.out.println(splitsSlavesRepartition);

		for (ThreadExecutor t: threads) {	
			t.join();  // Waits until the completion of each thread
			// We gather single (no duplication -> hashSet) words of each split file (-> of each thread)
			UMWordsRep.put("UM" + t.splitName.substring(1, t.splitName.length()), t.getSingleWords());
		}

		System.out.println("Dictionary UMx-Words");
		System.out.println(UMWordsRep);
		System.out.println("");
		return UMWordsRep;
	}

	
	/**
	 * This function does the shuffling then reducing procedures by calling methods shuffle and reduce of SlaveWork class via ThreadExecutor.
	 * @param UMWordsRep Dictionary of the list of words (value) for each UM file (key).
	 * @param slaves list of reachable hosts.
	 * @return 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static ArrayList<ThreadExecutor> shufflingReducing(HashMap<String, ArrayList<String>> UMWordsRep, ArrayList<String> slaves) throws IOException, InterruptedException 
	{
		System.out.println("******************** SHUFFLING AND REDUCING PROCEDURES ********************");
		HashMap<String, HashSet<String>> wordUMsRep = inverseDic(UMWordsRep);
		System.out.println("Dictionary Word-UMs");
		System.out.println(wordUMsRep);

		ArrayList<ThreadExecutor> threads = new ArrayList<ThreadExecutor>();
		HashMap<String, String> RMSlaveRepartition = new HashMap<String, String>();
		int indexSM = 0;
		int indexSlave = 0;

		// Creation of a Thread for each word 
		for (String word: wordUMsRep.keySet()) {
			// Constructor 2 (i.e., mode UMXSMX) of ThreadExecutor class
			ThreadExecutor runnable = new ThreadExecutor(join(wordUMsRep.get(word)), slaves.get(indexSlave), "UMXSMX", "SM" + indexSM, word.toLowerCase());
			RMSlaveRepartition.put("RM" + indexSM, slaves.get(indexSlave));
			runnable.start();
			threads.add(runnable);
			indexSM++;
			indexSlave++;
			if (indexSlave >= slaves.size()) {
				indexSlave = 0;
			}		
		}
		System.out.println("Dictionary RMx-Slave");
		System.out.println(RMSlaveRepartition +"\n");	
		return threads;
	}
	
	
	/**
	 * This function does the assembling procedure.
	 * @param threads list of opened threads for each word of the file.
	 * @param resultFile file where result of wordCount procedure will be written
	 * @return Dictionary of the number of occurences (value) of each word (key) present in the input file.
	 * @throws FileNotFoundException
	 * @throws InterruptedException
	 */
	public static HashMap<String, String> assembling (ArrayList<ThreadExecutor> threads, File resultFile) throws FileNotFoundException, InterruptedException 
	{
		System.out.println("******************** ASSEMBLING PROCEDURE ********************");
		PrintWriter writerFinal = new PrintWriter(resultFile);
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
		System.out.println("Word count dictionary result");
		System.out.println(wordCountDic + "\n");
		return wordCountDic;
	}

	// Insert a comma between every element of the input list
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

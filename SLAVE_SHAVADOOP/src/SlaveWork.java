import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SlaveWork {

	public static String fileDir = ("/cal/homes/mzenaidi/workspace/DATA_FILES");
	public static File forbiddenWordsFile = new File(fileDir, "motsIgnores.txt");


	public static void main(String[] args) throws IOException 
	{
		ArrayList<String> forbiddenWords = extractForbiddenWords(forbiddenWordsFile);

		switch (args[0]) 	
		{
		case "SXUMX": 
			map(new File(fileDir, args[1]), forbiddenWords);
			break;

		case "UMXSMX": 
			int counterWord = shuffle(new File(fileDir, args[2]), args[1], args[3]);
			reduce(args[1], counterWord, args[2]);
			break;

		default:
			break;
		}
	}

	/**
	 * Function that 
	 * @param split split file on which mapping is performed
	 * @param forbiddenWords list of forbidden words to be filtered for wordCount procedure (pronouns, special characters). 
	 * @throws IOException
	 */
	public static void map(File split, ArrayList<String> forbiddenWords) throws IOException 
	{
		BufferedReader br = new BufferedReader(new FileReader(split));
		PrintWriter writer = new PrintWriter(new File(fileDir, "UM"+split.getName().substring(1)));
		Set<String> singleWords = new HashSet<String>();
		String line = br.readLine();
		while(line != null) {
			String[] words = line.split(" ");
			for (String word: words) {
				word = refactor(word);
				if (!forbiddenWords.contains(word.toLowerCase())){
					singleWords.add(word.toLowerCase());
					writer.println(word.toLowerCase() + " 1");
				}
			}
			line = br.readLine();
		}

		for(String singleWord: singleWords) {
			System.out.println(singleWord);
		}

		br.close();
		writer.close();
	}

	/**
	 * 
	 * @param SM sorted map file to be created.
	 * @param word word to be processed.
	 * @param UMs UMs where word is present.
	 * @return count of word in initial input file.
	 * @throws IOException
	 */
	public static int shuffle(File SM, String word, String UMs) throws IOException 
	{
		String[] UMx = UMs.split(",");
		PrintWriter writerSM = new PrintWriter(SM);
		int counterWord = 0;
		for(String UM: UMx) {
			BufferedReader br = new BufferedReader(new FileReader(new File(fileDir, UM)));
			String lineR = br.readLine();
			while(lineR != null) {		
				if (lineR.split(" ")[0].equals(word))
				{
					writerSM.println(lineR);
					counterWord++;
				}
				lineR = br.readLine();
			}
			br.close();
		}
		writerSM.close();
		return counterWord;
	}

	public static void reduce(String word, int counterWord, String fileIdentifier) throws FileNotFoundException 
	{
		PrintWriter writerRM = new PrintWriter(new File(fileDir, "RM" + fileIdentifier.substring(2)));
		System.out.println(word + " " + counterWord);
		writerRM.println(word + " " + counterWord);
		writerRM.close();
	}

	/**
	 * Function that reads a file containing all words that has to be filtered for word count procedure and returns them in an ArrayList structure.
	 * @param forbiddenWordsFile file of forbidden words. 
	 * @return list of words that must be filtered for word count procedure.
	 * @throws IOException
	 */
	public static ArrayList<String> extractForbiddenWords(File forbiddenWordsFile) throws IOException 
	{
		ArrayList<String> forbiddenWordsList = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new FileReader(forbiddenWordsFile));
		String word = br.readLine();
		while (word != null) {
			forbiddenWordsList.add(word);
			word = br.readLine();
		}
		br.close();
		return forbiddenWordsList;
	}


	public static String refactor(String word) 
	{
		String pattern = "(\\w+)(\\.|,|;|:|!)";  // Add interrogation point
		Pattern r = Pattern.compile(pattern);
		Matcher m = r.matcher(word);
		if (m.find( )) {
			word =  word.replaceAll(word, m.group(1));
		}

		return word;
	}
}
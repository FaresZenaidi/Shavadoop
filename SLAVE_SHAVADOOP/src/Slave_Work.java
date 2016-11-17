import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Slave_Work {

	public static BufferedReader br;
	public static String fileDir = ("/cal/homes/mzenaidi/workspace/DATA_FILES");
	public static File forbiddenWordsFile = new File(fileDir, "motsIgnores.txt");

	public static void main(String[] args) throws IOException 
	{
		ArrayList<String> forbiddenWords = extractForbiddenWords(forbiddenWordsFile);

		switch (args[0]) 	
		{
		case "SXUMX": 
			br = new BufferedReader(new FileReader(new File(fileDir, args[1])));
			PrintWriter writer = new PrintWriter(new File(fileDir, "UM"+args[1].substring(1, args[1].length())));
			Set<String> keys = new HashSet<String>();
			String line = br.readLine();
			while(line != null) {
				String[] words = line.split(" ");
				for (String word: words) {
					word = refactor(word);
					if (!forbiddenWords.contains(word.toLowerCase())){
						keys.add(word.toLowerCase());
						writer.println(word.toLowerCase() + " 1");
					}
				}
				line = br.readLine();
			}

			for(String k: keys) {
				System.out.println(k);
			}

			br.close();
			writer.close();
			break;

		case "UMXSMX": 
			String[] UMx = args[3].split(",");
			ArrayList<String> linesWithKey = new ArrayList<String>();
			PrintWriter writerSM = new PrintWriter(new File(fileDir, args[2]));
			PrintWriter writerRM = new PrintWriter(new File(fileDir, "RM" + args[2].substring(2, args[2].length())));
			int counterKey = 0;
			for(String UM: UMx) {
				br = new BufferedReader(new FileReader(new File(fileDir, UM)));
				String lineR = br.readLine();
				while(lineR != null) {		
					if (lineR.split(" ")[0].equals(args[1]))
					{
						writerSM.println(lineR);
						linesWithKey.add(lineR);
						counterKey++;
					}
					lineR = br.readLine();
				}
			}
			br.close();
			writerSM.close();
			System.out.println(args[1] + " " + counterKey);
			writerRM.println(args[1] + " " + counterKey);
			writerRM.close();
			break;

		default:
			break;
		}
	}

	public static ArrayList<String> extractForbiddenWords(File forbiddenWordsFile) throws IOException 
	{
		ArrayList<String> forbiddenWordsList = new ArrayList<String>();
		br = new BufferedReader(new FileReader(forbiddenWordsFile));
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
		String pattern = "(\\w+)(\\.|,|;|:|!)";
		Pattern r = Pattern.compile(pattern);
		Matcher m = r.matcher(word);
		if (m.find( )) {
			word =  word.replaceAll(word, m.group(1));
		}

		return word;
	}

}

/*
public static void main(String[] args) {
		try {IOExceptionIOException
			Thread.sleep(10000);

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
 */
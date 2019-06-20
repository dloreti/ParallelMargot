package arg;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
import java.util.Vector;

public class PosTaggerManager {

	public TreeSet<String> wordSet;
	public HashMap<String,Integer> dictionary;
	public HashMap<String,Integer> bigramDictionary;
	public HashMap<String,Integer> trigramDictionary;
	public HashMap<String,Double> idf;
	
	public PosTaggerManager()
	{
		wordSet = new TreeSet<String>();
		dictionary = new HashMap<String,Integer>();
		bigramDictionary = new HashMap<String,Integer>();
		trigramDictionary = new HashMap<String,Integer>();
		idf = new HashMap<String,Double>();
	}

	public int getIndexByTrigram(String trigram)
	{
		Integer idx = trigramDictionary.get(trigram);
		if (idx != null) return idx.intValue();
		else return 0;
	}

	public int getIndexByBigram(String bigram)
	{
		Integer idx = bigramDictionary.get(bigram);
		if (idx != null) return idx.intValue();
		else return 0;
	}

	public int getIndexByWord(String word)
	{
		Integer idx = dictionary.get(word);
		if (idx != null) return idx.intValue();
		else return 0;
	}

	public void createWordSetFromSentenceCorpus(Vector<String> sc)
	{
		for (int i=0; i<sc.size(); i++)
		{
			String[] nodeContentTokens = sc.elementAt(i).split("\\s+");
			
			for (int k=0; k<nodeContentTokens.length; k++)
			{
				//String token = nodeContentTokens[k].replaceAll("\\p{P}", "");
				String token = nodeContentTokens[k];
				wordSet.add(token);
			}
		}		
	}
	
	public void createDictionary()
	{
		Integer count = 0;
		Iterator iter = wordSet.iterator();
		while (iter.hasNext()) {
			String word = iter.next().toString();
			if (!dictionary.containsKey(word))
			{
				dictionary.put(word,count);
				count++;
			}
		}
	}
	
	public void createBigramDictionary(Vector<String> sc)
	{
		Integer count = 0;
		
		String key = "";
		
		for (int k = 0; k < sc.size(); k++)
		{
			String[] tokens = sc.elementAt(k).split("\\s+");

			String first = "#";
			String second = "#";

			for (int w = 0; w < tokens.length; w++)
			{
				first = second;
				second = tokens[w];
	
				key = first+" "+second;
	
				if (!bigramDictionary.containsKey(key))
				{
					bigramDictionary.put(key,count);
					count++;
				}
			}
		}
	}
	
	public void createTrigramDictionary(Vector<String> sc)
	{
		Integer count = 0;
		
		String key = "";
		
		for (int k = 0; k < sc.size(); k++)
		{
			String[] tokens = sc.elementAt(k).split("\\s+");

			String first = "#";
			String second = "#";
			String third = "#";

			for (int w = 0; w < tokens.length; w++)
			{
				first = second;
				second = third;
				third = tokens[w];
	
				key = first+" "+second+" "+third;
	
				if (!trigramDictionary.containsKey(key))
				{
					trigramDictionary.put(key,count);
					count++;
				}
			}
		}
	}

	public void saveToTextFiles(String directory)
	{
		try
		{
			FileWriter fileWriter = new FileWriter(directory + File.separator + "dictionary.txt");
			BufferedWriter writer = new BufferedWriter(fileWriter);			
			for (Map.Entry<String, Integer> entry : dictionary.entrySet())
			{
			    writer.write(entry.getKey() + " " + entry.getValue() + "\n");
			}
			writer.close();

			fileWriter = new FileWriter(directory + File.separator + "bigramDictionary.txt");
			writer = new BufferedWriter(fileWriter);			
			for (Map.Entry<String, Integer> entry : bigramDictionary.entrySet())
			{
			    writer.write(entry.getKey() + " " + entry.getValue() + "\n");
			}
			writer.close();

			fileWriter = new FileWriter(directory + File.separator + "trigramDictionary.txt");
			writer = new BufferedWriter(fileWriter);			
			for (Map.Entry<String, Integer> entry : trigramDictionary.entrySet())
			{
			    writer.write(entry.getKey() + " " + entry.getValue() + "\n");
			}
			writer.close();

			fileWriter = new FileWriter(directory + File.separator + "idf.txt");
			writer = new BufferedWriter(fileWriter);			
			for (Map.Entry<String, Double> entry : idf.entrySet())
			{
			    writer.write(entry.getKey() + " " + entry.getValue() + "\n");
			}
			writer.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void loadFromTextFiles(String directory)
	{
		try
		{
			FileReader fileReader = new FileReader(directory + File.separator + "dictionary.txt");
			BufferedReader reader = new BufferedReader(fileReader);
			String line = reader.readLine();
			while (line != null)
			{
				String[] pair = line.split(" ");
				dictionary.put(pair[0], Integer.parseInt(pair[1]));
				line = reader.readLine();
			}
			reader.close();

			fileReader = new FileReader(directory + File.separator + "bigramDictionary.txt");
			reader = new BufferedReader(fileReader);
			line = reader.readLine();
			while (line != null)
			{
				String[] pair = line.split(" ");
				bigramDictionary.put(pair[0] + " " + pair[1], Integer.parseInt(pair[2]));
				line = reader.readLine();
			}
			reader.close();

			fileReader = new FileReader(directory + File.separator + "trigramDictionary.txt");
			reader = new BufferedReader(fileReader);
			line = reader.readLine();
			while (line != null)
			{
				String[] pair = line.split(" ");
				trigramDictionary.put(pair[0] + " " + pair[1] + " " + pair[2], Integer.parseInt(pair[3]));
				line = reader.readLine();
			}
			reader.close();

			fileReader = new FileReader(directory + File.separator + "idf.txt");
			reader = new BufferedReader(fileReader);
			line = reader.readLine();
			while (line != null)
			{
				String[] pair = line.split(" ");
				idf.put(pair[0], Double.parseDouble(pair[1]));
				line = reader.readLine();
			}
			reader.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}

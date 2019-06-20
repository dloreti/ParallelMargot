package arg;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
import java.util.Vector;

import org.tartarus.snowball.SnowballStemmer;

public class DictionaryManager implements Serializable{

	public TreeSet<String> wordSet;
	public TreeSet<String> wordSetNotStemmed;
	public HashMap<String,Integer> dictionary;
	public HashMap<String,Integer> dictionaryNotStemmed;
	public HashMap<String,Integer> bigramDictionary;
	public HashMap<String,Integer> trigramDictionary;
	public HashMap<String,Double> idf;
	public Class stemClass;
	public SnowballStemmer stemmer;
	
	public DictionaryManager()
	{
		wordSet = new TreeSet<String>();
		wordSetNotStemmed = new TreeSet<String>();
		dictionary = new HashMap<String,Integer>();
		dictionaryNotStemmed = new HashMap<String,Integer>();
		bigramDictionary = new HashMap<String,Integer>();
		trigramDictionary = new HashMap<String,Integer>();
		idf = new HashMap<String,Double>();
	}
	
	public int getIndexByWord(String word)
	{
		stemmer = null;
		try
		{
			stemClass = Class.forName("org.tartarus.snowball.ext.english" + "Stemmer");
			stemmer = (SnowballStemmer) stemClass.newInstance();
			stemmer.setCurrent(word);
			stemmer.stem();
			String stemmedWord = stemmer.getCurrent();
			Integer idx = dictionary.get(stemmedWord);
			if (idx != null) return idx.intValue();
			else return -1;
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return -1;
		}	
	}

	public int getIndexByWordNotStemmed(String word)
	{
		try
		{
			Integer idx = dictionaryNotStemmed.get(word);
			if (idx != null) return idx.intValue();
			else return -1;
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return -1;
		}	
	}

	public void createDictionaryFromWordVector(Vector<String> w)
	{
		Integer count = 0;
		
		for (int i=0; i<w.size(); i++)
		{
			String word = w.elementAt(i);
			if (!dictionary.containsKey(word))
			{
				dictionary.put(word,count);
				count++;
			}
		}
	}
	
	public void createWordSetFromSentenceCorpus(Vector<String> sc)
	{
		stemmer = null;
		try
		{
			stemClass = Class.forName("org.tartarus.snowball.ext.english" + "Stemmer");
			stemmer = (SnowballStemmer) stemClass.newInstance();
		} 
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		for (int i=0; i<sc.size(); i++)
		{
			String[] nodeContentTokens = sc.elementAt(i).split("\\s+");
			HashMap<String,Integer> localIdf = new HashMap<String,Integer>();
			
			for (int k=0; k<nodeContentTokens.length; k++)
			{
				String token = nodeContentTokens[k].replaceAll("\\p{P}", "");
				wordSetNotStemmed.add(token);
				stemmer.setCurrent(token);
				stemmer.stem();
				String stemmedToken = stemmer.getCurrent();
				wordSet.add(stemmedToken);
				
				localIdf.put(stemmedToken, 1);
			}

			// Update global idf with local idf
			Iterator iter = localIdf.keySet().iterator();
			while (iter.hasNext()) {
				String key = iter.next().toString();
				Double val = idf.get(key);
				if (val == null)
				{
					idf.put(key, 1.0);
				}
				else
				{
					idf.put(key, val + 1.0);
				}
			}
		}
		
		Iterator iter = idf.keySet().iterator();
		while (iter.hasNext()) {
			String key = iter.next().toString();
			Double val = idf.get(key);
			idf.put(key, Math.log10(idf.size()/val));
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

		count = 0;
		iter = wordSetNotStemmed.iterator();
		while (iter.hasNext()) {
			String word = iter.next().toString();
			if (!dictionaryNotStemmed.containsKey(word))
			{
				dictionaryNotStemmed.put(word,count);
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

			fileWriter = new FileWriter(directory + File.separator + "dictionaryNotStemmed.txt");
			writer = new BufferedWriter(fileWriter);			
			for (Map.Entry<String, Integer> entry : dictionaryNotStemmed.entrySet())
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
			stemClass = Class.forName("org.tartarus.snowball.ext.english" + "Stemmer");
			stemmer = (SnowballStemmer) stemClass.newInstance();

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

			fileReader = new FileReader(directory + File.separator + "dictionaryNotStemmed.txt");
			reader = new BufferedReader(fileReader);
			line = reader.readLine();
			while (line != null)
			{
				String[] pair = line.split(" ");
				dictionaryNotStemmed.put(pair[0], Integer.parseInt(pair[1]));
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

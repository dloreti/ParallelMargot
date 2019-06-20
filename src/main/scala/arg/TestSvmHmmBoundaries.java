package arg;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Vector;
import java.util.Arrays;

import edu.stanford.nlp.trees.PennTreeReader;
import edu.stanford.nlp.trees.Tree;

public class TestSvmHmmBoundaries {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		int windowFeatures = Integer.parseInt(args[0]);
		
		int halfWindowFeatures = (windowFeatures-1)/2;
		
		Vector<String> sentences = new Vector<String>();
		Vector<String> stemmedSentences = new Vector<String>();
		Vector<String> postagSentences = new Vector<String>();
		Vector<String> nerSentences = new Vector<String>();
		
		String sentencesFileName = args[1];
		String parseTreesFileName = args[2];
		String nerSentencesFileName = args[3];

		String dictionariesDirectory = args[4];
		String posDictionariesDirectory = args[5];
		String tagsDictionariesDirectory = args[6];
		String nerDictionariesDirectory = args[7];

		try
		{
			// READ SENTENCES
			FileReader sentenceReader = new FileReader(sentencesFileName);
			BufferedReader br = new BufferedReader(sentenceReader);
			String sentence = br.readLine();
			while (sentence != null)
			{
				sentences.add(sentence);
				sentence = br.readLine();
			}
			br.close();

			// READ PARSE TREES
			FileReader reader = new FileReader(parseTreesFileName);
			PennTreeReader ptr = new PennTreeReader(reader);
			Tree t = ptr.readTree();
			while (t != null)
			{
				String postagSentence = "";
				String stemmedSentence = "";

				for (int kk = 0; kk < t.preTerminalYield().toArray().length; kk++)
				{
					String postag = t.preTerminalYield().toArray()[kk].toString();
					String word = t.getLeaves().toArray()[kk].toString();

					postagSentence += postag + " ";
					stemmedSentence += word + " ";

				}

				postagSentences.add(postagSentence);
				stemmedSentences.add(stemmedSentence);

				t = ptr.readTree();
			}
			ptr.close();
			
			// READ NER SENTENCES
			FileReader nerSentenceReader = new FileReader(nerSentencesFileName);
			BufferedReader nerBr = new BufferedReader(nerSentenceReader);
			String nerSentence = nerBr.readLine();
			while (nerSentence != null)
			{
				nerSentences.add(nerSentence);
				nerSentence = nerBr.readLine();
			}
			nerBr.close();


		} catch(Exception e) {
			e.printStackTrace();
		}
		
		DictionaryManager dm = new DictionaryManager();
		dm.loadFromTextFiles(dictionariesDirectory);

		PosTaggerManager ptm = new PosTaggerManager();
		ptm.loadFromTextFiles(posDictionariesDirectory);

		PosTaggerManager tm = new PosTaggerManager();
		tm.loadFromTextFiles(tagsDictionariesDirectory);

		DictionaryManager nm = new DictionaryManager();
		nm.loadFromTextFiles(nerDictionariesDirectory);

		for (int i = 0; i < stemmedSentences.size(); i++)
		{
			String[] words = sentences.elementAt(i).split("\\s+");
			String[] postags = postagSentences.elementAt(i).split("\\s+");
			String[] nertags = nerSentences.elementAt(i).split("\\s+");

			while (nertags.length < words.length)
			{
				nertags  = Arrays.copyOf(nertags, nertags.length + 1);
    				nertags[nertags.length - 1] = "O";
			}

			int[] sentenceLabels = new int[words.length];
			String[] featureVectorStr = new String[words.length];
			for (int kk = 0; kk < words.length; kk++)
			{
				sentenceLabels[kk] = 1;
				featureVectorStr[kk] = "";

				int offset = 0;

				int idx = dm.getIndexByWord(words[kk]) + 1;
				if (idx != 0)
				{
					featureVectorStr[kk] += (offset + idx) + ":1.0 ";
				}
				offset += dm.dictionary.size() + 1;

				idx = dm.getIndexByWordNotStemmed(words[kk]) + 1;
				if (idx != 0)
				{
					featureVectorStr[kk] += (offset + idx) + ":1.0 ";
				}
				offset += dm.dictionaryNotStemmed.size() + 1;

				idx = ptm.getIndexByWord(postags[kk]) + 1;
				if (idx != 0)
				{
					featureVectorStr[kk] +=  (offset + idx) + ":1.0 ";
				}
				offset += ptm.dictionary.size() + 1;

				idx = nm.getIndexByWord(nertags[kk]) + 1;
				if (idx != 0)
				{
					featureVectorStr[kk] +=  (offset + idx) + ":1.0 ";
				}
				offset += nm.dictionary.size() + 1;

				if (kk-2 >= 0)
				{
					idx = ptm.getIndexByTrigram(postags[kk-2] + " " + postags[kk-1] + " " + postags[kk]) + 1;
					if (idx != 0)
					{
						featureVectorStr[kk] +=  (offset + idx) + ":1.0 ";
					}
				}
				offset += ptm.trigramDictionary.size() + 1;

				if (kk-2 >= 0)
				{
					idx = nm.getIndexByTrigram(nertags[kk-2] + " " + nertags[kk-1] + " " + nertags[kk]) + 1;
					if (idx != 0)
					{
						featureVectorStr[kk] +=  (offset + idx) + ":1.0 ";
					}
				}
				offset += nm.trigramDictionary.size() + 1;

				if (kk-1 >= 0 && kk+1 < words.length)
				{
					idx = ptm.getIndexByTrigram(postags[kk-1] + " " + postags[kk] + " " + postags[kk+1]) + 1;
					if (idx != 0)
					{
						featureVectorStr[kk] +=  (offset + idx) + ":1.0 ";
					}
				}
				offset += ptm.trigramDictionary.size() + 1;

				if (kk-1 >= 0 && kk+1 < words.length)
				{
					idx = nm.getIndexByTrigram(nertags[kk-1] + " " + nertags[kk] + " " + nertags[kk+1]) + 1;
					if (idx != 0)
					{
						featureVectorStr[kk] +=  (offset + idx) + ":1.0 ";
					}
				}
				offset += nm.trigramDictionary.size() + 1;

				if (kk+2 < words.length)
				{
					idx = ptm.getIndexByTrigram(postags[kk] + " " + postags[kk+1] + " " + postags[kk+2]) + 1;
					if (idx != 0)
					{
						featureVectorStr[kk] +=  (offset + idx) + ":1.0 ";
					}
				}
				offset += ptm.trigramDictionary.size() + 1;

				if (kk+2 < words.length)
				{
					idx = nm.getIndexByTrigram(nertags[kk] + " " + nertags[kk+1] + " " + nertags[kk+2]) + 1;
					if (idx != 0)
					{
						featureVectorStr[kk] +=  (offset + idx) + ":1.0 ";
					}
				}
				offset += nm.trigramDictionary.size() + 1;

				for (int w = 0; w < halfWindowFeatures; w++)
				{
					if (kk-w-1 >= 0)
					{
						idx = dm.getIndexByWord(words[kk-w-1]) + 1;
						if (idx != 0)
						{
							featureVectorStr[kk] +=  (offset + idx) + ":1.0 ";
						}
						offset += dm.dictionary.size() + 1;

						idx = dm.getIndexByWordNotStemmed(words[kk-w-1]) + 1;
						if (idx != 0)
						{
							featureVectorStr[kk] +=  (offset + idx) + ":1.0 ";
						}
						offset += dm.dictionaryNotStemmed.size() + 1;

						idx = ptm.getIndexByWord(postags[kk-w-1]) + 1;
						if (idx != 0)
						{
							featureVectorStr[kk] +=  (offset + idx) + ":1.0 ";
						}
						offset += ptm.dictionary.size() + 1;

						idx = nm.getIndexByWord(nertags[kk-w-1]) + 1;
						if (idx != 0)
						{
							featureVectorStr[kk] +=  (offset + idx) + ":1.0 ";
						}
						offset += nm.dictionary.size() + 1;
					}
				}

				for (int w = 0; w < halfWindowFeatures; w++)
				{
					if (kk+w+1 <= words.length-1)
					{
						idx = dm.getIndexByWord(words[kk+w+1]) + 1;
						if (idx != 0)
						{
							featureVectorStr[kk] +=  (offset + idx) + ":1.0 ";
						}
						offset += dm.dictionary.size() + 1;

						idx = dm.getIndexByWordNotStemmed(words[kk+w+1]) + 1;
						if (idx != 0)
						{
							featureVectorStr[kk] +=  (offset + idx) + ":1.0 ";
						}
						offset += dm.dictionaryNotStemmed.size() + 1;

						idx = ptm.getIndexByWord(postags[kk+w+1]) + 1;
						if (idx != 0)
						{
							featureVectorStr[kk] +=  (offset + idx) + ":1.0 ";
						}
						offset += ptm.dictionary.size() + 1;

						idx = nm.getIndexByWord(nertags[kk+w+1]) + 1;
						if (idx != 0)
						{
							featureVectorStr[kk] +=  (offset + idx) + ":1.0 ";
						}											
						offset += nm.dictionary.size() + 1;
					}
				}

			}

			for (int kk = 0; kk < words.length; kk++)
			{
				System.out.println(sentenceLabels[kk] + "\tqid:" + i + "\t" + featureVectorStr[kk]);
			}
		}
	}
}

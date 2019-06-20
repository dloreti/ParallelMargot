package arg;

import java.io.FileReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Vector;

import edu.stanford.nlp.trees.PennTreeReader;
import edu.stanford.nlp.trees.PennTreebankLanguagePack;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreePrint;

public class TestBoW {

	public static Vector<String> trees;
	public static Vector<String> sentences;
	public static Vector<String> stemmedSentences;
	public static Vector<String> postagSentences;
	public static Vector<Integer> labels;
	public static DictionaryManager dm;
	public static DictionaryManager dmStemmed;
	public static PosTaggerManager ptm;
	public static FeatureExtractor fe;
	public static HashMap<String,Integer> classes;

	public static void main(String[] args) {

		//TreePrint tpOneline = new TreePrint("oneline", "stem", new PennTreebankLanguagePack());
		TreePrint tpStemmedWords = new TreePrint("words", "stem", new PennTreebankLanguagePack());
		//TreePrint tpWords = new TreePrint("words");
		//TreePrint tpWordsAndTags = new TreePrint("wordsAndTags");
		
		String treesFile = args[0];
		String stemmedDictionariesDirectory = args[1];
		
		labels = new Vector<Integer>();
		trees = new Vector<String>();
		sentences = new Vector<String>();
		postagSentences = new Vector<String>();
		stemmedSentences = new Vector<String>();
		
		dmStemmed = new DictionaryManager();
		dm = new DictionaryManager();
		ptm = new PosTaggerManager();
		
		//dm.loadFromTextFiles(dictionariesDirectory);
		//ptm.loadFromTextFiles(posDictionariesDirectory);
		dmStemmed.loadFromTextFiles(stemmedDictionariesDirectory);
		
		fe = new FeatureExtractor();

		try
		{
			FileReader reader = new FileReader(treesFile);
			PennTreeReader ptr = new PennTreeReader(reader);
			Tree tree = ptr.readTree();

			while (tree != null)
			{
				StringWriter sw = new StringWriter();
				PrintWriter pw = new PrintWriter(sw);
				tpStemmedWords.printTree(tree,pw);
				stemmedSentences.add(sw.toString());
/*
				sw = new StringWriter();
				pw = new PrintWriter(sw);
				tpWords.printTree(tree,pw);
				sentences.add(sw.toString());

				sw = new StringWriter();
				pw = new PrintWriter(sw);
				tpWordsAndTags.printTree(tree,pw);
				String str = "";
				String[] splitted = sw.toString().split(" ");
				for (int k=0; k<splitted.length; k++)
				{
					str += splitted[k].split("/")[1] + " ";
				}
				postagSentences.add(str);

				sw = new StringWriter();
				pw = new PrintWriter(sw);
				tpOneline.printTree(tree,pw);
				trees.add(sw.toString());
*/
				tree = ptr.readTree();
			}

			ptr.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}


		//dm.createWordSetFromSentenceCorpus(sentences);
		//dm.createDictionary();
		//dm.createBigramDictionary(sentences);
		//dm.createTrigramDictionary(sentences);

		dmStemmed.createWordSetFromSentenceCorpus(stemmedSentences);
		dmStemmed.createDictionary();
		dmStemmed.createBigramDictionary(stemmedSentences);
		dmStemmed.createTrigramDictionary(stemmedSentences);

		//ptm.createWordSetFromSentenceCorpus(postagSentences);
		//ptm.createDictionary();
		//ptm.createBigramDictionary(postagSentences);
		//ptm.createTrigramDictionary(postagSentences);
		
		Vector<SparseVector> featureVectors = fe.createTFIDFBagOfWords(stemmedSentences, dmStemmed);

		for (int k = 0 ; k < featureVectors.size(); k++)
		{
			System.out.println(featureVectors.elementAt(k).toSVM());
		}
	}
}

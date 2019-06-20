package arg;

import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;

public class FeatureExtractor {

	public FeatureExtractor()
	{
		
	}

	public Vector<SparseVector> createBagOfWords(Vector<String> sentences, DictionaryManager dm)
	{
		Vector<SparseVector> v = new Vector<SparseVector>();
		
		for (int i = 0; i < sentences.size(); i++)
		{
			String sentence = sentences.elementAt(i);
			
			String[] tokens = sentence.split("\\s+");

			SparseVector s = new SparseVector();
			
			for (int k = 0; k < tokens.length; k++)
			{
				String token = tokens[k].replaceAll("\\p{P}", "");
				dm.stemmer.setCurrent(token);
				dm.stemmer.stem();
				String stemmedToken = dm.stemmer.getCurrent();
				Integer index = dm.dictionary.get(stemmedToken);
				
				if (index != null)
				{
					index++;
					s.increment(index, 1.0);
				}
			}

			//s.normalize(tokens.length);

			v.add(s);
		}
		
		return v;
	}
	
	public Vector<SparseVector> createBooleanBagOfWords(Vector<String> sentences, DictionaryManager dm)
	{
		Vector<SparseVector> v = new Vector<SparseVector>();
		
		for (int i = 0; i < sentences.size(); i++)
		{
			String sentence = sentences.elementAt(i);
			
			String[] tokens = sentence.split("\\s+");

			SparseVector s = new SparseVector();
			
			for (int k = 0; k < tokens.length; k++)
			{
				String token = tokens[k].replaceAll("\\p{P}", "");
				dm.stemmer.setCurrent(token);
				dm.stemmer.stem();
				String stemmedToken = dm.stemmer.getCurrent();
				Integer index = dm.dictionary.get(stemmedToken);
				
				if (index != null)
				{
					index++;
					s.insert(index, 1.0);
				}
			}

			//s.normalize(tokens.length);

			v.add(s);
		}
		
		return v;
	}	

	public Vector<SparseVector> createBooleanBagOfWordsAndPostags(Vector<String> sentences, Vector<String> posTagsSentences, DictionaryManager dm, PosTaggerManager ptm)
	{
		Vector<SparseVector> v = new Vector<SparseVector>();
		
		for (int i = 0; i < sentences.size(); i++)
		{
			String sentence = sentences.elementAt(i);
			
			String[] tokens = sentence.split("\\s+");

			SparseVector s = new SparseVector();
			
			for (int k = 0; k < tokens.length; k++)
			{
				String token = tokens[k].replaceAll("\\p{P}", "");
				dm.stemmer.setCurrent(token);
				dm.stemmer.stem();
				String stemmedToken = dm.stemmer.getCurrent();
				Integer index = dm.dictionary.get(stemmedToken);
				
				if (index != null)
				{
					index++;
					s.insert(index, 1.0);
				}
			}

			v.add(s);
		}
		
		int dictionarySize = dm.dictionary.size();
		
		for (int i = 0; i < posTagsSentences.size(); i++)
		{
			String posTagSentence = posTagsSentences.elementAt(i);
			
			String[] tokens = posTagSentence.split("\\s+");

			SparseVector s = new SparseVector();
			
			for (int k = 0; k < tokens.length; k++)
			{
				String token = tokens[k].replaceAll("\\p{P}", "");
				Integer index = ptm.dictionary.get(token);
				
				if (index != null)
				{
					index++;
					s.increment(dictionarySize + index, 1.0);
				}
			}
			
			v.add(s);
		}
		
		return v;
	}	

	//ORIGINAL - MARCO LIPPI :
	public Vector<SparseVector> createTFIDFBagOfWords(Vector<String> sentences, DictionaryManager dm) {
		Vector<SparseVector> v = new Vector<SparseVector>();
		
		for (int i = 0; i < sentences.size(); i++)
		{
			String sentence = sentences.elementAt(i);
			
			String[] tokens = sentence.split("\\s+");

			SparseVector s = new SparseVector();
			SparseVector idf = new SparseVector();
			
			for (int k = 0; k < tokens.length; k++)
			{
				String token = tokens[k].replaceAll("\\p{P}", "");
				Integer index = null;
				String stemmedToken = null;

				dm.stemmer.setCurrent(token);
				dm.stemmer.stem();
				stemmedToken = dm.stemmer.getCurrent();
				index = dm.dictionary.get(stemmedToken);

				if (index != null)
				{
					index++;
					s.increment(index, 1.0);
					Double idfval = null;
					idfval = dm.idf.get(stemmedToken); 

					if (idfval != null)
					{
						idf.insert(index, idfval.doubleValue());
					}
				}
			}

			// After normalization s contains TF
			s.normalize(tokens.length);
			
			// Now multiply for IDF
			s.pointwiseMult(idf);
			
			v.add(s);
		}
		
		return v;
	}

	//DANIELA LORETI:
	public HashMap<String,SparseVector> createTFIDFBagOfWords(HashMap<String,String> sentences, DictionaryManager dm)
	{
		//Vector<SparseVector> v = new Vector<SparseVector>();
		HashMap<String,SparseVector> v = new HashMap<String, SparseVector>();

		for (HashMap.Entry<String, String> entry : sentences.entrySet()) {
			//for (int i = 0; i < sentences.size(); i++){
			//String sentence = sentences.elementAt(i);
			String sentence = entry.getValue();

			String[] tokens = sentence.split("\\s+");

			SparseVector s = new SparseVector();
			SparseVector idf = new SparseVector();

			for (int k = 0; k < tokens.length; k++)
			{
				String token = tokens[k].replaceAll("\\p{P}", "");
				Integer index = null;
				String stemmedToken = null;

				dm.stemmer.setCurrent(token);
				dm.stemmer.stem();
				stemmedToken = dm.stemmer.getCurrent();
				index = dm.dictionary.get(stemmedToken);

				if (index != null)
				{
					index++;
					s.increment(index, 1.0);
					Double idfval = null;
					idfval = dm.idf.get(stemmedToken);

					if (idfval != null)
					{
						idf.insert(index, idfval.doubleValue());
					}
				}
			}

			// After normalization s contains TF
			s.normalize(tokens.length);

			// Now multiply for IDF
			s.pointwiseMult(idf);

			//v.add(s);
			v.put(entry.getKey(),s);
		}

		return v;
	}

	//DANIELA LORETI:
	public ConcurrentHashMap<String,SparseVector> createTFIDFBagOfWords(ConcurrentHashMap<String,String> sentences, DictionaryManager dm)
	{
		//Vector<SparseVector> v = new Vector<SparseVector>();
		ConcurrentHashMap<String,SparseVector> v = new ConcurrentHashMap<String, SparseVector>();

		for (HashMap.Entry<String, String> entry : sentences.entrySet()) {
			//for (int i = 0; i < sentences.size(); i++){
			//String sentence = sentences.elementAt(i);
			String sentence = entry.getValue();

			String[] tokens = sentence.split("\\s+");

			SparseVector s = new SparseVector();
			SparseVector idf = new SparseVector();

			for (int k = 0; k < tokens.length; k++)
			{
				String token = tokens[k].replaceAll("\\p{P}", "");
				Integer index = null;
				String stemmedToken = null;

				dm.stemmer.setCurrent(token);
				dm.stemmer.stem();
				stemmedToken = dm.stemmer.getCurrent();
				index = dm.dictionary.get(stemmedToken);

				if (index != null)
				{
					index++;
					s.increment(index, 1.0);
					Double idfval = null;
					idfval = dm.idf.get(stemmedToken);

					if (idfval != null)
					{
						idf.insert(index, idfval.doubleValue());
					}
				}
			}

			// After normalization s contains TF
			s.normalize(tokens.length);

			// Now multiply for IDF
			s.pointwiseMult(idf);

			//v.add(s);
			v.put(entry.getKey(),s);
		}

		return v;
	}

	public SparseVector createTFIDFBagOfWords(String tree, DictionaryManager dm)
	{

			String[] tokens = tree.split("\\s+");

			SparseVector s = new SparseVector();
			SparseVector idf = new SparseVector();

			for (int k = 0; k < tokens.length; k++)
			{
				String token = tokens[k].replaceAll("\\p{P}", "");
				Integer index = null;
				String stemmedToken = null;

				dm.stemmer.setCurrent(token);
				dm.stemmer.stem();
				stemmedToken = dm.stemmer.getCurrent();
				index = dm.dictionary.get(stemmedToken);

				if (index != null)
				{
					index++;
					s.increment(index, 1.0);
					Double idfval = null;
					idfval = dm.idf.get(stemmedToken);

					if (idfval != null)
					{
						idf.insert(index, idfval.doubleValue());
					}
				}
			}

			// After normalization s contains TF
			s.normalize(tokens.length);

			// Now multiply for IDF
			s.pointwiseMult(idf);


		return s;
	}

	public Vector<SparseVector> createBooleanPosTagsBagOfWords(Vector<String> posTagsSentences, PosTaggerManager ptm)
	{
		Vector<SparseVector> v = new Vector<SparseVector>();
		
		for (int i = 0; i < posTagsSentences.size(); i++)
		{
			String posTagSentence = posTagsSentences.elementAt(i);
			
			String[] tokens = posTagSentence.split("\\s+");

			SparseVector s = new SparseVector();
			SparseVector idf = new SparseVector();
			
			for (int k = 0; k < tokens.length; k++)
			{
				String token = tokens[k].replaceAll("\\p{P}", "");
				Integer index = ptm.dictionary.get(token);
				
				if (index != null)
				{
					index++;
					s.insert(index, 1.0);
				}
			}

			//s.normalize(tokens.length);
			
			v.add(s);
		}
		
		return v;
	}

	public Vector<SparseVector> createPosTagsBagOfWords(Vector<String> posTagsSentences, PosTaggerManager ptm)
	{
		Vector<SparseVector> v = new Vector<SparseVector>();
		
		for (int i = 0; i < posTagsSentences.size(); i++)
		{
			String posTagSentence = posTagsSentences.elementAt(i);
			
			String[] tokens = posTagSentence.split("\\s+");

			SparseVector s = new SparseVector();
			SparseVector idf = new SparseVector();
			
			for (int k = 0; k < tokens.length; k++)
			{
				String token = tokens[k].replaceAll("\\p{P}", "");
				Integer index = ptm.dictionary.get(token);
				
				if (index != null)
				{
					index++;
					s.increment(index, 1.0);
				}
			}

			//s.normalize(tokens.length);
			
			v.add(s);
		}
		
		return v;
	}

	public Vector<SparseVector> createBigramsBagOfWords(Vector<String> sentences, DictionaryManager dm)
	{
		Vector<SparseVector> v = new Vector<SparseVector>();
		
		for (int i = 0; i < sentences.size(); i++)
		{
			String sentence = sentences.elementAt(i);
			
			String[] tokens = sentence.split("\\s+");

			SparseVector s = new SparseVector();
			
			String first = "#";
			String second = "#";
			
			String key = "";
			
			for (int k = 0; k < tokens.length; k++)
			{
				first = second;
				second = tokens[k].replaceAll("\\p{P}", "");
				
				key = first+" "+second;

				Integer index = dm.bigramDictionary.get(key);

				if (index != null)
				{
					index++;
					s.increment(index, 1.0);
				}
			}

			//s.normalize(tokens.length);
			
			v.add(s);
		}
		
		return v;
	}

	public Vector<SparseVector> createTrigramsBagOfWords(Vector<String> sentences, DictionaryManager dm)
	{
		Vector<SparseVector> v = new Vector<SparseVector>();
		
		for (int i = 0; i < sentences.size(); i++)
		{
			String sentence = sentences.elementAt(i);
			
			String[] tokens = sentence.split("\\s+");

			SparseVector s = new SparseVector();
			
			String first = "#";
			String second = "#";
			String third = "#";
			
			String key = "";
			
			for (int k = 0; k < tokens.length; k++)
			{
				first = second;
				second = third;
				third = tokens[k].replaceAll("\\p{P}", "");
				
				key = first+" "+second+" "+third;

				Integer index = dm.trigramDictionary.get(key);

				if (index != null)
				{
					index++;
					s.increment(index, 1.0);
				}
			}

			//s.normalize(tokens.length);
			
			v.add(s);
		}
		
		return v;
	}

	public Vector<SparseVector> createBigramsBagOfPosTags(Vector<String> posTagsSentences, PosTaggerManager ptm)
	{
		Vector<SparseVector> v = new Vector<SparseVector>();
		
		for (int i = 0; i < posTagsSentences.size(); i++)
		{
			String sentence = posTagsSentences.elementAt(i);
			
			String[] tokens = sentence.split("\\s+");

			SparseVector s = new SparseVector();
			
			String first = "#";
			String second = "#";
			
			String key = "";
			
			for (int k = 0; k < tokens.length; k++)
			{
				first = second;
				second = tokens[k].replaceAll("\\p{P}", "");
				
				key = first+" "+second;

				Integer index = ptm.bigramDictionary.get(key);

				if (index != null)
				{
					index++;
					s.increment(index, 1.0);
				}
			}

			//s.normalize(tokens.length);
			
			v.add(s);
		}
		
		return v;
	}

	public Vector<SparseVector> createTrigramsBagOfPosTags(Vector<String> posTagsSentences, PosTaggerManager ptm)
	{
		Vector<SparseVector> v = new Vector<SparseVector>();
		
		for (int i = 0; i < posTagsSentences.size(); i++)
		{
			String sentence = posTagsSentences.elementAt(i);
			
			String[] tokens = sentence.split("\\s+");

			SparseVector s = new SparseVector();
			
			String first = "#";
			String second = "#";
			String third = "#";
			
			String key = "";
			
			for (int k = 0; k < tokens.length; k++)
			{
				first = second;
				second = third;
				third = tokens[k].replaceAll("\\p{P}", "");
				
				key = first+" "+second+" "+third;

				Integer index = ptm.trigramDictionary.get(key);

				if (index != null)
				{
					index++;
					s.increment(index, 1.0);
				}
			}

			//s.normalize(tokens.length);
			
			v.add(s);
		}
		
		return v;
	}
	
	public Vector<SparseVector> createTrigramsBagOfWordsAndPosTags(Vector<String> sentences, Vector<String> posTagsSentences, DictionaryManager dm, PosTaggerManager ptm)
	{
		Vector<SparseVector> s1 = createTrigramsBagOfWords(sentences,dm);
		Vector<SparseVector> s2 = createTrigramsBagOfPosTags(posTagsSentences,ptm);
		int offset = dm.trigramDictionary.size();
		for (int i=0; i<s1.size(); i++)
		{
			s1.elementAt(i).combine(s2.elementAt(i), offset);
		}
		
		return s1;
	}

	public Vector<SparseVector> createTFIDFAndPosTagsBagOfWords(Vector<String> sentences, Vector<String> posTagsSentences, DictionaryManager dm, PosTaggerManager ptm)
	{
		Vector<SparseVector> s1 = createTFIDFBagOfWords(sentences,dm);
		Vector<SparseVector> s2 = createPosTagsBagOfWords(posTagsSentences,ptm);
		int offset = dm.dictionary.size();
		for (int i=0; i<s1.size(); i++)
		{
			s1.elementAt(i).combine(s2.elementAt(i), offset);
		}
		
		return s1;
	}

	public Vector<SparseVector> createFeatureVectorWords(Vector<String> sentences, DictionaryManager dm)
	{
		Vector<SparseVector> s1 = createBooleanBagOfWords(sentences,dm);
		Vector<SparseVector> s2 = createBigramsBagOfWords(sentences,dm);
		Vector<SparseVector> s3 = createTrigramsBagOfWords(sentences,dm);
		
		int offset1 = dm.dictionary.size();
		int offset2 = dm.bigramDictionary.size();

		for (int i=0; i<s1.size(); i++)
		{
			s1.elementAt(i).combine(s2.elementAt(i), offset1);
			s1.elementAt(i).combine(s3.elementAt(i), offset1+offset2);
		}

		return s1;
	}
	
	public Vector<SparseVector> createCompleteFeatureVector(Vector<String> sentences, Vector<String> posTagsSentences, DictionaryManager dm, PosTaggerManager ptm)
	{
		Vector<SparseVector> s1 = createBooleanBagOfWords(sentences,dm);
		Vector<SparseVector> s2 = createPosTagsBagOfWords(posTagsSentences,ptm);
		Vector<SparseVector> s3 = createBigramsBagOfWords(sentences,dm);
		Vector<SparseVector> s4 = createBigramsBagOfPosTags(posTagsSentences,ptm);
		Vector<SparseVector> s5 = createTrigramsBagOfWords(sentences,dm);
		Vector<SparseVector> s6 = createTrigramsBagOfPosTags(posTagsSentences,ptm);
		
		int offset1 = dm.dictionary.size();
		int offset2 = ptm.dictionary.size();
		int offset3 = dm.bigramDictionary.size();
		int offset4 = ptm.bigramDictionary.size();
		int offset5 = dm.trigramDictionary.size();
		int offset6 = ptm.trigramDictionary.size();

		for (int i=0; i<s1.size(); i++)
		{
			s1.elementAt(i).combine(s2.elementAt(i), offset1);
			s1.elementAt(i).combine(s3.elementAt(i), offset1+offset2);
			s1.elementAt(i).combine(s4.elementAt(i), offset1+offset2+offset3);
			s1.elementAt(i).combine(s5.elementAt(i), offset1+offset2+offset3+offset4);
			s1.elementAt(i).combine(s6.elementAt(i), offset1+offset2+offset3+offset4+offset5);
		}

		return s1;
	}
	
	public Vector<SparseVector> createCompleteFeatureVectorThreeDictionaries(Vector<String> sentences, Vector<String> posTagsSentences, Vector<String> nerSentences, DictionaryManager dm, PosTaggerManager ptm, DictionaryManager dnm)
	{
		Vector<SparseVector> s1 = createTFIDFBagOfWords(sentences,dm);
		//Vector<SparseVector> s1 = createBagOfWords(sentences,dm);
		Vector<SparseVector> s2 = createPosTagsBagOfWords(posTagsSentences,ptm);
		Vector<SparseVector> s3 = createBagOfWords(nerSentences,dnm);
		Vector<SparseVector> s4 = createBigramsBagOfWords(sentences,dm);
		Vector<SparseVector> s5 = createBigramsBagOfPosTags(posTagsSentences,ptm);
		Vector<SparseVector> s6 = createBigramsBagOfWords(nerSentences,dnm);
		Vector<SparseVector> s7 = createTrigramsBagOfWords(sentences,dm);
		Vector<SparseVector> s8 = createTrigramsBagOfPosTags(posTagsSentences,ptm);
		Vector<SparseVector> s9 = createTrigramsBagOfWords(nerSentences,dnm);
		
		int offset1 = dm.dictionary.size();
		int offset2 = ptm.dictionary.size();
		int offset3 = dnm.dictionary.size();
		int offset4 = dm.bigramDictionary.size();
		int offset5 = ptm.bigramDictionary.size();
		int offset6 = dnm.bigramDictionary.size();
		int offset7 = dm.trigramDictionary.size();
		int offset8 = ptm.trigramDictionary.size();
		int offset9 = dnm.trigramDictionary.size();

		for (int i=0; i<s1.size(); i++)
		{
			int offset = offset1;
			//s1.elementAt(i).combine(s2.elementAt(i), offset);
			offset += offset2;
			//s1.elementAt(i).combine(s3.elementAt(i), offset);
			offset += offset3;
			//s1.elementAt(i).combine(s4.elementAt(i), offset);
			offset += offset4;
			//s1.elementAt(i).combine(s5.elementAt(i), offset);
			offset += offset5;
			//s1.elementAt(i).combine(s6.elementAt(i), offset);
			offset += offset6;
			//s1.elementAt(i).combine(s7.elementAt(i), offset);
			offset += offset7;
			//s1.elementAt(i).combine(s8.elementAt(i), offset);
			offset += offset8;
			//s1.elementAt(i).combine(s9.elementAt(i), offset);
			
			s1.elementAt(i).normalize(s1.elementAt(i).norm());
		}


		return s1;
	}

}

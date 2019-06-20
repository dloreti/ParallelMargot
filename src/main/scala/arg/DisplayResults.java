package arg;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Vector;

import edu.stanford.nlp.trees.PennTreeReader;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreePrint;

public class DisplayResults {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		Vector<Double> outputClaim = new Vector<Double>();
		Vector<Double> outputEvidence = new Vector<Double>();
		Vector<String> sentences = new Vector<String>();
		Vector<String> inputSvmHmmClaimSentences = new Vector<String>();
		Vector<String> inputSvmHmmEvidenceSentences = new Vector<String>();
		Vector<Integer> outputSvmHmmClaim = new Vector<Integer>();
		Vector<Integer> outputSvmHmmEvidence = new Vector<Integer>();
		Vector<Integer> lengthSvmHmmClaim = new Vector<Integer>();
		Vector<Integer> lengthSvmHmmEvidence = new Vector<Integer>();
		
		String line = "";

		try
		{
			FileReader outputClaimFileReader = new FileReader(args[0]);
			BufferedReader br = new BufferedReader(outputClaimFileReader);
			line = br.readLine();

			while (line != null)
			{
				outputClaim.add(Double.parseDouble(line));
				line = br.readLine();
			}
			br.close();
			
			FileReader outputEvidenceFileReader = new FileReader(args[1]);
			br = new BufferedReader(outputEvidenceFileReader);
			line = br.readLine();

			while (line != null)
			{
				outputEvidence.add(Double.parseDouble(line));
				line = br.readLine();
			}
			br.close();

			FileReader sentencesFileReader = new FileReader(args[2]);
			br = new BufferedReader(sentencesFileReader);
			line = br.readLine();

			while (line != null)
			{
				sentences.add(line);
				line = br.readLine();
			}
			br.close();

			FileReader inputSvmHmmClaimSentencesFileReader = new FileReader(args[3]);
			br = new BufferedReader(inputSvmHmmClaimSentencesFileReader);
			line = br.readLine();

			while (line != null)
			{
				lengthSvmHmmClaim.add(line.split("\\s+").length);
				inputSvmHmmClaimSentences.add(line);
				line = br.readLine();
			}
			br.close();

			FileReader inputSvmHmmEvidenceSentencesFileReader = new FileReader(args[4]);
			br = new BufferedReader(inputSvmHmmEvidenceSentencesFileReader);
			line = br.readLine();

			while (line != null)
			{
				lengthSvmHmmEvidence.add(line.split("\\s+").length);
				inputSvmHmmEvidenceSentences.add(line);
				line = br.readLine();
			}
			br.close();
			
			FileReader outputSvmHmmClaimFileReader = new FileReader(args[5]);
			br = new BufferedReader(outputSvmHmmClaimFileReader);
			line = br.readLine();

			while (line != null)
			{
				outputSvmHmmClaim.add(Integer.parseInt(line));
				line = br.readLine();
			}
			br.close();
			
			FileReader outputSvmHmmEvidenceFileReader = new FileReader(args[6]);
			br = new BufferedReader(outputSvmHmmEvidenceFileReader);
			line = br.readLine();

			while (line != null)
			{
				outputSvmHmmEvidence.add(Integer.parseInt(line));
				line = br.readLine();
			}
			br.close();

			String randomString = args[7];
			String txtFilename = randomString + ".txt";
			String jsonFilename = randomString + ".json";
			String xmlFilename = randomString + ".xml";

			String xmlText = "";

			PrintWriter txtWriter = new PrintWriter(txtFilename);
			PrintWriter jsonWriter = new PrintWriter(jsonFilename);
			PrintWriter xmlWriter = new PrintWriter(xmlFilename);

			xmlText += "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
			xmlText += "<document>";

			int claimCount = 0;
			int evidenceCount = 0;
			int offsetClaim = 0;
			int offsetEvidence = 0;
			boolean alreadyStarted = false;
			
			JSONArray sentencesJson = new JSONArray();

			for (int i=0; i<sentences.size(); i++)
			{
				txtWriter.print("SENTENCE CLAIM_SCORE:" + outputClaim.elementAt(i) + " EVIDENCE_SCORE:" + outputEvidence.elementAt(i) + " TEXT:" + sentences.elementAt(i) + "\n");
				xmlText += "<sentence><claim_score>" + outputClaim.elementAt(i) + "</claim_score><evidence_score>" + outputEvidence.elementAt(i) + "</evidence_score><text>" + sentences.elementAt(i) + "</text>\n";

				JSONObject objJson = new JSONObject();
				objJson.put("claim_score",outputClaim.elementAt(i));
				objJson.put("evidence_score",outputEvidence.elementAt(i));
				objJson.put("text",sentences.elementAt(i));

				if (outputClaim.elementAt(i) > 0)
				{
					String[] claimWords = inputSvmHmmClaimSentences.elementAt(claimCount).split("\\s+");
					
					if (outputEvidence.elementAt(i) > 0)
					{
						alreadyStarted = false;
						String claim_evidence = "";

						// Print claim_evidence
						for (int j=0; j<claimWords.length; j++)
						{
							if (outputSvmHmmClaim.elementAt(offsetClaim+j) == 1 && outputSvmHmmEvidence.elementAt(offsetEvidence+j) == 1)
							{
								if (!alreadyStarted)
								{
									txtWriter.print("CLAIM_EVIDENCE " + claimWords[j]);
									xmlText += "<claim_evidence> " + claimWords[j];
									claim_evidence += claimWords[j];
									alreadyStarted = true;
								}
								else
								{
									txtWriter.print(" " + claimWords[j]);
									xmlText += " " + claimWords[j];
									claim_evidence += " " + claimWords[j];
								}
							}
							else if (alreadyStarted)
							{
								txtWriter.print("\n");
								xmlText += "</claim_evidence>\n";
								alreadyStarted = false;
							}							
						}
						
						if (alreadyStarted)
						{
							txtWriter.print("\n");
							xmlText += "</claim_evidence>\n";
							alreadyStarted = false;
						}							
				
						objJson.put("claim_evidence",claim_evidence);
					}
					else
					{
						String claim = "";
						alreadyStarted = false;

						// Print claim
						for (int j=0; j<claimWords.length; j++)
						{
							if (outputSvmHmmClaim.elementAt(offsetClaim+j) == 1)
							{
								if (!alreadyStarted)
								{
									txtWriter.print("CLAIM " + claimWords[j]);
									xmlText += "<claim> " + claimWords[j];
									claim += claimWords[j];
									alreadyStarted = true;
								}
								else
								{
									txtWriter.print(" " + claimWords[j]);
									xmlText += " " + claimWords[j];
									claim += " " + claimWords[j];
								}
							}
							else if (alreadyStarted)
							{
								txtWriter.print("\n");
								xmlText += "</claim>\n";
								alreadyStarted = false;
							}							
						}

						if (alreadyStarted)
						{
							txtWriter.print("\n");
							xmlText += "</claim>\n";
							alreadyStarted = false;
						}

						objJson.put("claim",claim);
					}
					
					offsetClaim += claimWords.length;
					claimCount++;
				}
				if (outputEvidence.elementAt(i) > 0)
				{
					String[] evidenceWords = inputSvmHmmEvidenceSentences.elementAt(evidenceCount).split("\\s+");

					String evidence = "";
					alreadyStarted = false;
					
					// Print evidence
					for (int j=0; j<evidenceWords.length; j++)
					{
						if (outputSvmHmmEvidence.elementAt(offsetEvidence+j) == 1)
						{
							if (!alreadyStarted)
							{
								txtWriter.print("EVIDENCE " + evidenceWords[j]);
								xmlText += "<evidence> " + evidenceWords[j];
								evidence += evidenceWords[j];
								alreadyStarted = true;
							}
							else
							{
								txtWriter.print(" " + evidenceWords[j]);
								xmlText += " " + evidenceWords[j];
								evidence += " " + evidenceWords[j];
							}
						}
						else if (alreadyStarted)
						{
							txtWriter.print("\n");
							xmlText += "</evidence>\n";
							alreadyStarted = false;
						}							
					}

					if (alreadyStarted)
					{
						txtWriter.print("\n");
						xmlText += "</evidence>\n";
						alreadyStarted = false;
					}
			
					objJson.put("evidence",evidence);

					offsetEvidence += evidenceWords.length;
					evidenceCount++;
				}
				if (outputClaim.elementAt(i) < 0 && outputEvidence.elementAt(i) < 0)
				{
					txtWriter.print("NOT_ARGUMENTATIVE " + sentences.elementAt(i) + "\n");
					xmlText += "<not_argumentative> " + sentences.elementAt(i) + "</not_argumentative>\n";
				}

				sentencesJson.put(objJson);

				xmlText += "</sentence>\n";
			}

			xmlText += "</document>";
			xmlWriter.print(xmlText);

			JSONObject mainJson = new JSONObject();
			mainJson.put("document", sentencesJson);
			jsonWriter.print(mainJson);

			txtWriter.close();
			jsonWriter.close();
			xmlWriter.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}


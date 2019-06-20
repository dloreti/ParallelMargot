package arg;

import java.io.BufferedReader;
import java.io.FileReader;

import net.sf.junidecode.Junidecode;

public class Unidecoder {

	public Unidecoder ()
	{
		
	}
	
	public static void main(String[] args)
	{
		try
		{
			FileReader fileReader = new FileReader(args[0]);
			BufferedReader br = new BufferedReader(fileReader);
			String line = br.readLine();
			
			while (line != null)
			{
				line = Junidecode.unidecode(line);
				//line = java.text.Normalizer.normalize(line, java.text.Normalizer.Form.NFC);
				//line = line.replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
				
				System.out.println(line);
				
				line = br.readLine();
			}
			
			br.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}

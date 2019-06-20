package arg;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class SparseVector {

	public TreeMap<Integer,Double> data;
	
	public SparseVector()
	{
		data = new TreeMap<Integer,Double>();
	}

	public SparseVector(String str)
	{
		data = new TreeMap<Integer,Double>();
		String[] featuresStr = str.split("\\s+");
		for (int kk = 0; kk < featuresStr.length; kk++)
		{
			data.put(kk+1,Double.parseDouble(featuresStr[kk]));
		}
	}

	public void incrementIndex(Integer offset){
		TreeMap<Integer,Double> dataUpd = new TreeMap<Integer,Double>();
		for(Map.Entry<Integer,Double> entry : data.entrySet()) {
			dataUpd.put(entry.getKey()+offset, entry.getValue());
		}
		data=dataUpd;
	}

	public void insert(Integer index, Double value)
	{
		data.put(index, value);
	}

	public void increment(Integer index, Double value)
	{
		Double v = (Double)(data.get(index));
		if (v == null)
		{
			data.put(index, value);
		}
		else
		{
			data.put(index, v + value);			
		}
	}
	
	public void pointwiseMult(SparseVector s)
	{
		Iterator iter = data.keySet().iterator();
		while (iter.hasNext())
		{
			Integer key = (Integer)(iter.next());
			Double value = data.get(key);
			Double value2 = s.data.get(key);
			if (value2 == null) value2 = 0.0;
			
			data.put(key, value*value2);
		}	
	}
	
	public void normalize(double size)
	{
		Iterator iter = data.keySet().iterator();
		while (iter.hasNext())
		{
			Integer key = (Integer)(iter.next());
			Double value = data.get(key);
			data.put(key, value/size);
		}
	}
	
	public Double dotProduct(SparseVector other)
	{
		Double dot = 0.0;
		
		if (data.size() <= other.data.size()) {
            for (int i : data.keySet())
                if (other.data.containsKey(i)) dot += data.get(i) * other.data.get(i);
        }
        else  {
            for (int i : other.data.keySet())
                if (data.containsKey(i)) dot += data.get(i) * other.data.get(i);
        }
		
		return dot;
	}
	
	public Double norm()
	{
		Double norm = 0.0;
		
		for (int i : data.keySet())
		{
			norm += data.get(i)*data.get(i);
		}
		
		return Math.sqrt(norm);
	}

	public Double cosineSimilarity(SparseVector other)
	{
		return dotProduct(other)/(norm()*other.norm());
	}

	public void combine(SparseVector other, int offset)
	{
		Iterator iter = other.data.keySet().iterator();
		while (iter.hasNext())
		{
			Integer key = (Integer)(iter.next());
			Double value = other.data.get(key);
			data.put(key + offset, value);
		}
	}

	public String toSVM()
	{
		String ret = "";
		Iterator iter = data.keySet().iterator();
		while (iter.hasNext())
		{
			Integer key = (Integer)(iter.next());
			Double value = data.get(key);
			ret += key + ":" + value + " "; 
		}
		
		return ret;
	}
}

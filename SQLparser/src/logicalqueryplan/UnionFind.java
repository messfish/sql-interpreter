package logicalqueryplan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cs4321.support.Catalog;

/**
 * this class is the class that implement the union find data structure.
 * @author jz699 JUNCHEN ZHAN
 *
 */
public class UnionFind {

	protected Map<String, String> union; // this stores the union find data structure.
	protected Map<String, Mule> root; // this stores the root to the distinct unions.
	
	/**
	 * constructor: build the union find data structure.
	 * @param array the array contains a list of table names.
	 */
	public UnionFind(String[] array, Map<String, String> hash){
		union = new HashMap<>();
		root = new HashMap<>();
		Catalog catalog = Catalog.getInstance();
		for(int i=0;i<array.length;i++){
			List<String> column = catalog.getList(hash.get(array[i]));
			for(String s : column){
				String str = array[i] + "." + s;
			    union.put(str," ");
			}
		}
	}
	
	/**
	 * find the root of the table name.
	 * recursively call this method until we find the value when it is empty.
	 * return that table name.
	 * @param s the table name that is the start of searching.
	 * @return the table that is at the root.
	 */
	public String findRoot(String s){
	    if(union.get(s).equals(" ")) return s;
	    return findRoot(union.get(s));
	}
	
	/**
	 * this method set the union hash map.
	 * the second string is the back pointer of the first one.
	 * @param s1 the first table name.
	 * @param s2 the second table name which is the back pointer of the first one.
	 */
	public void Set(String s1, String s2){
		union.put(s2, s1);
	}
	
	/**
	 * this method returns the union data structure by the given String.
	 * @param s the string serves as the key.
	 * @return the union data structure.
	 */
	public Mule getMule(String s){
		if(!root.containsKey(s)) return null;
		return root.get(s);
	}
	
	/**
	 * this method assign the lower bound.
	 * @param mule the union data structure that will be modified.
	 * @param lb the integer contains the lower bound.
	 */
	public void setLower(Mule mule, Integer lb){
		if(mule.lb==null) mule.lb = lb;
		else mule.lb = Math.max(mule.lb,lb);
	}
	
	/**
	 * this method assign the upper bound.
	 * @param mule the union data structure that will be modified.
	 * @param up the integer contains the higher bound.
	 */
	public void setHigher(Mule mule, Integer up){
		if(mule.up==null) mule.up = up;
		else mule.up = Math.min(mule.up, up);
	}
	
	/**
	 * this method assign the equal bound.
	 * Note we will have to modify the upper and lower bound.
	 * @param mule the union data structure that will be modified.
	 * @param lb the integer contains the equal bound.
	 */
	public void setEqual(Mule mule, Integer ec){
		mule.ec = ec;
		mule.lb = ec;
		mule.up = ec;
	}
	
	/**
	 * this method assign the table element to the union structure.
	 * @param mule the union data structure that will be modified.
	 * @param s the string contains a table attribute.
	 */
	public void addAttribute(Mule mule, String s){
		mule.list.add(s);
	}
	
	/**
	 * return the union find data structure.
	 * @return the union find data structure.
	 */
	public Map<String, String> getMap() {
		return union;
	}
}

package cs4321.support;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cs4321.operator.ScanOperator;
import cs4321.operator.Tuple;
import cs4321.project2.Interpreter;

/** 
 * This is a supporting class telling the source files and tables,
 * the possible schemas for different tables and build the statistical data
 * from the given data. Also it builds the B+ Tree based on the index file.
 * @author jz699 JUNCHEN ZHEN
 *
 */
public class Catalog {

	
	private static final String base = "/db/data/"; // the base tables for the input.
	private static final String scheme = "/db/schema.txt";
	// this is the location of the schema.
	protected Map<String, String> map; // use a map to link to the file location.
	private Map<String, List<String>> list; // this map links the table to the column;
	private Map<String, Integer> schema; // use a map to link to the schema.
	private Map<String, Map<String, Integer>> index;
	// use an index map to link the table with their respective schema.
	private static Catalog instance = new Catalog(); // the instance to be returned.
	private static final String indexing = "/db/index_info.txt";
	// the file location to the index file.
	private Map<String, Integer> cluster; // this tells whether the table is clustered or not.
	private Map<String, Integer> order; // this tells the order of the B+ Tree.
	private Set<String> marked; // this tells whether the column is the index one.
	private Map<String, String> map2; // this connects the index to the table.
	private Map<String, double[]> V; // this stores the statistical data.
	
	/**
	 * the constructor of the Catalog. It parse the scheme and set the map.
	 * Build the statistical file and the B+ Tree by specific indices.
	 */
	private Catalog(){
		map = new HashMap<>();
		index = new HashMap<>();
		cluster = new HashMap<>();
		order = new HashMap<>();
		marked = new HashSet<>();
		map2 = new HashMap<>();
		list = new HashMap<>();
		V = new HashMap<>();
		String s;
		try{
			FileReader file = new FileReader(Interpreter.getInput()+scheme);
		    BufferedReader br = new BufferedReader(file);
		    while((s = br.readLine())!=null){
		    	String[] str = s.split("\\s+");
		    	map.put(str[0], Interpreter.getInput()+base+str[0]);
		    	schema = new HashMap<>();
		    	list.put(str[0], new ArrayList<String>());
		    	for(int i=1;i<str.length;i++){
		    		schema.put(str[0]+"."+str[i], i-1);
		    		list.get(str[0]).add(str[i]);
		    	}
		    	index.put(str[0], schema);
		    }
		    br.close();
			File file1 = new File(Interpreter.getInput()+indexing);
			BufferedReader buffer = new BufferedReader(new FileReader(file1));
			String s1;
			while((s1=buffer.readLine())!=null){
				String[] str = s1.split("\\s+");
				String temp = str[0] + "." + str[1];
				int index1 = Integer.parseInt(str[2]);
				int index2 = Integer.parseInt(str[3]);
				marked.add(temp);
				cluster.put(temp, index1);
				order.put(temp, index2);
				map2.put(temp, str[1]);
			}
			buffer.close();
		}catch(IOException e){
			System.out.println("Files not found!");
		}
		build();
		readData(V);
	}
	
	/**
	 * get the instance object.
	 * @return the Catalog object instance.
	 */
	public static Catalog getInstance(){
		return instance;
	}
	
	/**
	 * return the path of the file using the table name.
	 * @param s the input of the table name.
	 * @return the file location.
	 */
	public String getFileLocation(String s){
		if(!map.containsKey(s)) return null;
		return map.get(s);
	}
	
	/**
	 * return the column location of the tuple using the column name. 
	 * @param s is the input of the column name.
	 * @return the column location in the tuple
	 */
	public int getColumn(String s) {
		String[] duck = s.split("\\.");
		if(!index.containsKey(duck[0])) return -1;
		Map<String,Integer> temp = index.get(duck[0]);
		return temp.get(s);
	}
	
	/**
	 * the getter method of schema hash map.
	 * @return the schema hash map.
	 */
	public Map<String,Integer> getSchema(String s) {
		return index.get(s);
	}
	
	/**
	 * return whether the table is clustered or not.
	 * @param s the table name to check.
	 * @return 0 shows the B+ Tree is not clustered, 
	 * 1 shows the B+ Tree is clustered.
	 */
	public int getCluster(String s){
		if(!cluster.containsKey(s)) return -1;
		return cluster.get(s);
	}
	
	/**
	 * return the order of the B+ Tree for the specified table.
	 * @param s the table name to check.
	 * @return the order of the tree.
	 */
	public int getOrder(String s){
		return order.get(s);
	}
	
	/**
	 * return whether this column is the index.
	 * @param s the table and column name to check the result.
	 * @return whether the column is in the index.
	 */
	public boolean hasColumn(String s){
		return marked.contains(s);
	}
	
	/**
	 * return the index of the specific table.
	 * @param s the name of that table.
	 * @return the index of that table.
	 */
	public String getIndex(String s) {
		return map2.get(s);
	}
	
	/**
	 * return the list of columns for the given table name.
	 * @param s the String indicates the table name.
	 * @return the list of column names.
	 */
	public List<String> getList(String s) {
		if(!list.containsKey(s)) return null;
		return list.get(s);
	}

	/**
	 * this method returns the statistical data as hash map.
	 * @return the statistical data.
	 */
	public Map<String, double[]> getStat() {
		return V;
	}
	
	/**
	 * this method read the statistic file. Use the data from that file and the union find
	 * to determine the V-value for each attributes and the final number of tuples in the 
	 * single relation table and store them in a hash map.
	 * @param V the hash map that will be used to store all the data.
	 */
	private void readData(Map<String, double[]> V){
		File file = new File(Interpreter.getInput() + "/db/stats.txt");
		try {
			BufferedReader buffer = new BufferedReader(new FileReader(file));
			String s = null;
			while((s=buffer.readLine())!=null){
				String[] temp = s.split("\\s+");
				int nums = Integer.parseInt(temp[1]);
				V.put(temp[0],new double[]{0,nums});
				for(int i=2;i<temp.length;i++){
					String[] dummy = temp[i].split(",");
					int low = Integer.parseInt(dummy[1]);
					int high = Integer.parseInt(dummy[2]);
					V.put(temp[0]+"."+dummy[0], new double[]{low,high});
				}
			}
			buffer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}	
	}
	
	/**
	 * build the statistics file as required.
	 * @return the file contains the statistics.
	 */
	private File build(){
		File file = new File(Interpreter.getInput() + "/db/stats.txt");
		try{
			StringBuilder sb = new StringBuilder();
			BufferedWriter buffer = new BufferedWriter(new FileWriter(file));
			Map<String,String> dummy = map;
			for(Map.Entry<String, String> entry : dummy.entrySet()){
				File temp = new File(entry.getValue());
				ScanOperator scan = new ScanOperator(temp);
				int size = scan.getLength(), nums = 0;
				int[] max = new int[size], min = new int[size];
				for(int i=0;i<size;i++){
					max[i] = Integer.MIN_VALUE;
					min[i] = Integer.MAX_VALUE;
				}
				Tuple tuple = null;
				while((tuple=scan.getNextTuple())!=null){
					for(int i=0;i<size;i++){
						int compare = tuple.getData(i);
						max[i] = Math.max(max[i], compare);
						min[i] = Math.min(min[i], compare);
					}
					nums++;
				}
				sb.append(entry.getKey()).append(" ").append(nums);
				List<String> list = getList(entry.getKey());
				for(int i=0;i<list.size();i++){
					sb.append(" ").append(list.get(i));
					sb.append(",").append(min[i]);
					sb.append(",").append(max[i]);
				}
				sb.append("\n");
			}
			buffer.write(sb.toString());
			buffer.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		return file;
	}
	
}

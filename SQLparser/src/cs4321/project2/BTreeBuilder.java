package cs4321.project2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import cs4321.operator.Operator;
import cs4321.operator.ScanOperator;
import cs4321.operator.SortOperator;
import cs4321.operator.Tuple;
import cs4321.support.Catalog;


/**
 * this class handles the construction of the B+ tree.
 * It is a static tree by using the bulk loading method to insert data.
 * @author jz699 JUNCHEN ZHAN
 *
 */
public class BTreeBuilder {

	private Catalog catalog = Catalog.getInstance();
	
	/**
	 * this method get all those lines from one indexes and put them 
	 * in their specific files.
	 */
	public void dump(){
		String location = Interpreter.getInput() + "/db/index_info.txt";
		String output = Interpreter.getInput() + "/db/indexes/";
		File file = new File(location);
		try{
			BufferedReader buff = new BufferedReader(new FileReader(file));
			String s;
			while((s=buff.readLine())!=null){
				String[] str = s.split("\\s+");
				String temp = str[0] + "." + str[1];
				int point = catalog.getColumn(temp);
				int cluster = catalog.getCluster(temp);
				int order = catalog.getOrder(temp);
				construct(str[0],str[1],point,cluster,order,output+str[0]+"."+str[1]);
			}
			buff.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * This method creates a B+ Tree based on the requirement.
	 * generate a tree map to handle all those indexes of the tuples
	 * and use a bulk loading algrorithm to put out an result file.
	 * @param location the table name which will be used to create a B+ Tree.
	 * @param column the column that shall be used for indexing.
	 * @param point the index of the specific column
	 * @param cluster the flag indicates whether the B+ Tree is clustered or not.
	 * @param order the order of the B+ Trees.
	 * @param output the location of the output of the file.
	 * @return the file that shall be used for indexing.
	 */
	public File construct(String location, String column, int point, int cluster, int order, String output) {
		List<List<Integer>> copy = new ArrayList<>();
		// this is the temporay array which stores the indexes of the B+ Tree.
		copy.add(new ArrayList<Integer>());
		File file = new File(output);
		File target = new File(catalog.getFileLocation(location));
		Operator scan = new ScanOperator(target);
		if(cluster==1){
			String wholeColumn = location + "." + column;
			String[] array = {wholeColumn};
			scan = new SortOperator(scan,array,catalog.getSchema(location));
			scan.dump(catalog.getFileLocation(location), "");
			scan = new ScanOperator(target);
		}
		TreeMap<Integer, List<int[]>> tree = new TreeMap<>();
		buildTree(tree,scan,point);
		try{
			FileOutputStream fout = new FileOutputStream(file);
		    FileChannel fc = fout.getChannel();
		    ByteBuffer buffer = ByteBuffer.allocate(4096);
		    int size = tree.size();
		    int numofLeaves = (size-1)/(order*2) + 1;
		    int sum = numofLeaves;
		    buffer.putInt(4,numofLeaves);
		    boolean isFinished = false;
		    while(numofLeaves>1||!isFinished){
		    	numofLeaves = (numofLeaves-1)/(order*2+1) + 1;
		    	sum += numofLeaves;
		    	isFinished = true;
		    }
		    buffer.putInt(0,sum);
		    buffer.putInt(8,order);
		    buffer.position(0);
		    fc.write(buffer);
		    int nums = buildLeaves(fc,tree,order,copy);
		    buildIndexes(fc,order,nums,copy);
		    fout.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		return file;
	}
	
	/**
	 * This method get all the tuples from the operator, put their page index and bytebuffer 
	 * index to the tree. The tree will be used for the building of the B+ Tree.
	 * @param tree the tree map that holds the temporary data.
	 * @param scan the scan operator for building the tree.
	 * @param point the pointer of the index.
	 */
	private void buildTree(TreeMap<Integer, List<int[]>> tree, Operator scan, int point) {
		Tuple tuple = null;
		int pageindex = 0, pointer = 2;
		while((tuple=scan.getNextTuple())!=null){
			int index = tuple.getData(point);
			if(!tree.containsKey(index)) tree.put(index, new ArrayList<int[]>());
			int index1 = pageindex, index2 = (pointer-2)/tuple.length();
			if(pointer+tuple.length()<=1024)
				pointer += tuple.length();
			else{
				pageindex++;
				index1 = pageindex;
				pointer = 2 + tuple.length();
				index2 = 0;
			}
			tree.get(index).add(new int[]{index1,index2});
		}
	}
	
	/**
	 * this method generates all the leaves of the B+ Tree, iteratively create 
	 * all those indexes and put them in the maximum order of the B+ Tree.
	 * if the last one is underflowed, evenly split the last two leaf nodes.
	 * put all the result into the sepecified file.
	 * @param fc the file channel for this file.
	 * @param tree the tree map that shall be used.
	 * @param order the order of the B+ Tree.
	 * @param copy the array list that stores the traverse of the B+ Tree.
	 * @return the number of index pages.
	 */
	private int buildLeaves(FileChannel fc, TreeMap<Integer, List<int[]>> tree,
			int order, List<List<Integer>> copy) {
		int size = tree.size() - 1;
		boolean isUnderflow = size%(order*2) < order - 1 ;
		int nums = size/(order*2);
		if(isUnderflow) nums--;
		for(int i=0;i<nums;i++)
			writeLeafPage(fc,tree,order*2,copy);
		if(isUnderflow&&nums!=-1){
			int temp = tree.size()/2;
			writeLeafPage(fc,tree,temp,copy);
			nums++;
		}
		int dummy = tree.size();
		writeLeafPage(fc,tree,dummy,copy);
		return nums==-1? 1 : nums+1;
	}
	
	/**
	 * This method writes a leaf page by the instructions. 
	 * @param fc the file channel for this file.
	 * @param tree the tree map that shall be used.
	 * @param order the order of the B+ Tree.
	 * @param copy the array list that stores the traverse of the B+ Tree.
	 */
	private void writeLeafPage(FileChannel fc, TreeMap<Integer, List<int[]>> tree,
			int order, List<List<Integer>> copy) {
		List<Integer> list = new ArrayList<>(2);
		list.add(0);
		ByteBuffer buffer = ByteBuffer.allocate(4096);
		buffer.putInt(4,order);
		int index = 8;
		for(int j=0;j<order;j++){
		    int key = tree.firstKey();
		    if(j==0) list.add(key);
		    List<int[]> value = tree.remove(key); 
		    buffer.putInt(index, key);
		    index += 4;
		    buffer.putInt(index,value.size());
		    index += 4;
		    for(int k=0;k<value.size();k++){
		    	int[] dummy = value.get(k);
		    	buffer.putInt(index,dummy[0]);
		    	index += 4;
		    	buffer.putInt(index,dummy[1]);
		    	index += 4;
		    }
		}
		buffer.position(0);
		try {
			fc.write(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		}
		copy.add(list);
	}
	
	/**
	 * this method generates all the index pages for the B+ Tree. Assign all those
	 * indexes in the order from the instruction
	 * @param fc the file channel handles writing
	 * @param order the order of the B+ Tree.
	 * @param nums the initial starting number.
	 * @param copy the array list that stores the traverse of the B+ Tree.
	 */
	private void buildIndexes(FileChannel fc, int order, int nums, List<List<Integer>> copy) {
		int start = 1, dummy = nums + 1;
		boolean isFinished = false; 
		// use this variable to avoid one page condition.
		while(dummy-start>1||!isFinished){
			int num = dummy - start - 1;
			boolean isUnderFlow = num%(order*2+1) < order;
			int size = num/(order*2+1);
			if(isUnderFlow) size--;
			for(int i=0;i<size;i++){
				writeIndexPage(fc,order*2,start,copy);
				start += order*2 + 1;
			}
			int leftover = dummy - start, half = 0;
			if(isUnderFlow&&size>=0){
				half = leftover/2;
				writeIndexPage(fc,half-1,start,copy);
				start += half;
				size++;
			}
			writeIndexPage(fc,leftover-half-1,start,copy);
			start = dummy;
			size = size<0? 1: size + 1;
			dummy += size;
			isFinished = true;
		}
	}
	
	/**
	 * This page writes an index page by following the instructions.
	 * @param fc the file channel handles writing.
	 * @param order the order of the B+ Tree.
	 * @param start the index of the previous level page.
	 * @param copy the array list that stores the traverse of the B+ Tree.
	 */
	private void writeIndexPage(FileChannel fc, int order, int start, List<List<Integer>> copy) {
		List<Integer> list = new ArrayList<>(2);
		list.add(1);
		ByteBuffer buffer = ByteBuffer.allocate(4096);
		buffer.putInt(0,1);
		buffer.putInt(4,order);
		int log = start, point = 8;
		list.add(start);
		start++;
		for(int i=0;i<order;i++){
			List<Integer> temp = copy.get(start);
			while(temp.get(0)==1){
				temp = copy.get(temp.get(1));
			}
			buffer.putInt(point,temp.get(1));
			point += 4;
			start++;
		}
		while(log<start){
			buffer.putInt(point,log);
			point += 4;
			log++;
		}
		buffer.position(0);
		try {
			fc.write(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		}
		copy.add(list);
	}
	
	/**
	 * this method tries to get the number of leaf pages in a B+ Tree.
	 * @param file the file that stores the result in a B+ Tree.
	 * @return the number of leaves in a B+ Tree.
	 */
	public static int getNumberOfLeaves(File file){
		int result = 0;
		try {
			RandomAccessFile random = new RandomAccessFile(file,"r");
			FileChannel fc = random.getChannel();
			ByteBuffer buffer = ByteBuffer.allocate(4096);
			fc.read(buffer);
			result = buffer.getInt(4);
			random.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
}

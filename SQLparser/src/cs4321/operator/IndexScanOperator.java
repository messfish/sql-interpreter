package cs4321.operator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import cs4321.project2.Interpreter;
import cs4321.support.Catalog;

/**
 * this class handles the usage of B+ Tree by setting a low value
 * and a high value, then retrive the tuples between these two values.
 * @author jz699 JUNCHEN ZHAN
 *
 */
public class IndexScanOperator extends Operator{

	private long fcposition; // set the postion of the file channel.
	private int index; // set the index of the byte buffer.
	private long startPosition; // get the low position of the file channel.
	private int startIndex; // get the low index of the file channel.
	private double high; // get the high value of the file channel.
	private File file; // the file to store the B+ Tree.
	private RandomAccessFile fout; // the file output reader of the B+ Tree.
	private FileChannel fc; // the file channal for the B+ Tree.
	private ByteBuffer buffer; // the byte buffer for the file channel.
	private int cluster; // this index indicates whether the tree is clustered or not.
	private ScanOperator scan; // this is a ScanOperator.
	private boolean isAvailable = true; // this indicates whether there are tuples left.
	private int point; // the index that shall be used for the B+ Tree.
	private int size; // the number of available data in a leaf node.
	private int chaser; // the pointer that points to the available data in leaf node.
	private String table; // the table name of the B+ Tree.
	private Catalog catalog = Catalog.getInstance();
	
	/**
	 * constructor: set the lower and upper bound of the B+ Tree.
	 * within the bounds are the tuples satisfied the request.
	 * @param table the relation that will be queried.
	 * @param column the index that shall be used for the B+ Tree.
	 * @param low the lower bound for the B+ Tree.
	 * @param high the higher bound for the B+ Tree.
	 */
	public IndexScanOperator(String table, String column, double low, double high) {
		this.high = high;
		this.table = table;
		String combine = table + "." + column;
		point = catalog.getColumn(combine);
		cluster = catalog.getCluster(table);
		file = new File(Interpreter.getInput()+"/db/indexes/" + table + "." + column);
		traverse(file,low);
		try {
			fout = new RandomAccessFile(file, "r");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		fc = fout.getChannel();
		buffer = ByteBuffer.allocate(4096);
		fcposition = startPosition;
		index = startIndex + 8;
		scan = new ScanOperator(new File(catalog.getFileLocation(table)));
		try {
			fc.position(fcposition*4096);
			fc.read(buffer);
			if(buffer.getInt(0)==1) isAvailable = false;
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		scan.setPosition(buffer.getInt(startIndex+8), buffer.getInt(startIndex+12));
		size = buffer.getInt(startIndex+4);
	}

	/**
	 * return the next tuple for this operator.
	 * @return the next tuple.
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple tuple = scan.getNextTuple();
		if(tuple==null||tuple.getData(point)>=high){
			try {
				fout.close();
				if(tuple!=null) scan.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			return null;
		}
		if(isAvailable){
			if(cluster==1){
				if(tuple!=null&&tuple.getData(point)<high) return tuple;
			}else{
				chaser++;
				if(chaser<size){
					index += 8;
					scan.setPosition(buffer.getInt(index), buffer.getInt(index+4));
				}else{
					index += 8;
					if(index<4096&&buffer.getInt(index+4)!=0){
						size = buffer.getInt(index+4);
						chaser = 0;
						index += 8;
						if(scan.setPosition(buffer.getInt(index), buffer.getInt(index+4))==false)
							return null;
					}else{
						try {
							buffer.position(0);
							fc.read(buffer);
							if(buffer.getInt(0)!=1){
								chaser = 0;
								index = 8;
								size = buffer.getInt(index+4);
								index += 8;
								scan.setPosition(buffer.getInt(index), buffer.getInt(index+4));
							}
							else isAvailable = false;
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
				if(tuple!=null&&tuple.getData(point)<high){
					return tuple; 
				}
			}
		}
		return null;
	}

	/**
	 * reset the pointer to the start of the scanning.
	 */
	@Override
	public void reset() {
		fcposition = startPosition;
		index = startIndex + 8;
		scan = new ScanOperator(new File(catalog.getFileLocation(table)));
		try {
			fout = new RandomAccessFile(file, "r");
			fc = fout.getChannel();
			buffer = ByteBuffer.allocate(4096);
			fc.position(fcposition*4096);
			fc.read(buffer);
			isAvailable = true;
			if(buffer.getInt(0)==1) isAvailable = false;
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		scan.setPosition(buffer.getInt(startIndex+8), buffer.getInt(startIndex+12));
		size = buffer.getInt(startIndex+4);
	}
	
	/**
	 * this method tries to get the start position of the index, 
	 * assign them to appropiate files.
	 * @param file the file shows the serialized tree.
	 * @param temp the integer indicates the lower index.
	 */
	private void traverse(File file, double temp) {
        try{
        	RandomAccessFile fin = new RandomAccessFile(file,"r");
        	FileChannel fc = fin.getChannel();
        	ByteBuffer buffer = ByteBuffer.allocate(4096);
        	fc.read(buffer);
        	int start = buffer.getInt(0);
        	fc.position(start*4096);
        	buffer.position(0);
        	fc.read(buffer);
        	while(buffer.getInt(0)==1){
        		int limit = buffer.getInt(4), point = 8;
        		boolean isFound = false;
        		for(int i=0;i<limit;i++){
        			if(temp<buffer.getInt(point)){
            			int next = buffer.getInt(point+4*limit);
            			fc.position(next*4096);
            			buffer.position(0);
            			fc.read(buffer);
            			isFound = true;
            			startPosition = next; 
            			break;
            		}
        			point += 4;
        		}
        		if(!isFound){
        			int next = buffer.getInt(point+4*limit);
        			fc.position(next*4096);
        			buffer.position(0);
        			fc.read(buffer);
        			startPosition = next;
        		}
        	}
        	int size = buffer.getInt(4), index = 8;
        	boolean isSmaller = false;
        	for(int i=0;i<size;i++){
        		if(temp>=buffer.getInt(index)){
        			int nums = buffer.getInt(index+4);
        			index += nums * 8 + 8;
        		}
        		else{
        			startIndex = index;
        			isSmaller = true;
        			break;
        		}
        	}
        	if(!isSmaller){
        		startPosition += 1;
        		startIndex = 8;
        	}
        	fin.close();
        }catch(Exception e){
        	e.printStackTrace();
        }
	}

}

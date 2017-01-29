package cs4321.operator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * This class scans the whole table and type out by tuples.
 * @author jz699 JUNCHEN ZHAN
 *
 */
public class ScanOperator extends Operator implements TupleReader, MoveBack {

	private File file; // the file object that needs to be taken.
	private RandomAccessFile br; // the reader object.
	private FileChannel fc; // the file channel to be used for random access file.
	private ByteBuffer byt; // the byte buffer that will be used.
	private int index; // the index indicates which byte will be read.
	private int length; // the length of the tuple.
	private int volume; // the number of tuples this page has.
	private long anchor1; // the number serve as a temporary point.
	private int anchor2; // the number serve as a temporary point.
	
	/**
	 * Constructor: set the file to the ScanOperator
	 * and the buffered reader.
	 * @param file the input file.
	 */
	public ScanOperator(File file){
		this.file = file;
		try{
		    br = new RandomAccessFile(this.file, "r");
		    fc = br.getChannel();
		    byt = readPage();
		}catch(FileNotFoundException e){
			System.out.println("File not found!");
		}
		index = 8;
		if(byt!=null){
		    length = byt.getInt(0);
		    volume = byt.getInt(4);
		}
	}
	
	/**
	 * get the next Tuple of the scan.
	 * @return the Tuple class.
	 */
	@Override
	public Tuple getNextTuple() {
		// TODO Auto-generated method stub
		Tuple tuple = null;
		if(byt==null) return null;
		// check whether we have reached the end of this page.
		if(index<length*volume*4+8){
			int[] data = new int[length];
			for(int i=0;i<length;i++){
				data[i] = byt.getInt(index);
				index += 4;
			}
			tuple = new Tuple(data);
		}else{
			byt = readPage(); // read a new page.
			if(byt==null){
				try {
					br.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return null; // reach the end of file, return null.
			}
			index = 8;
			volume = byt.getInt(4); // stupid mistake! it is getInt(), not get()!
			int[] data = new int[length];
			for(int i=0;i<length;i++){
				data[i] = byt.getInt(index);
				index += 4;
			}
			tuple = new Tuple(data);
		}
		return tuple;
	}
	
	/**
	 * for the sake of reset, take down the point of the tuple.
	 */
	@Override
	public void anchor(){
		try {
			anchor1 = fc.position() - 4096;
			anchor2 = index - length*4;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * set the index back for the sake of sort merge join.
	 */
	@Override
	public void moveback(){
		try {
			fc.position(anchor1);
			byt = ByteBuffer.allocate(4096);
			fc.read(byt);
			index = anchor2;
			volume = byt.getInt(4); 
			// remember to set the volume to appropriate number.
			// otherwise, there might be a potential bug!
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * reset the pointer to the start point.
	 */
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		try{
			br = new RandomAccessFile(this.file, "r");
			fc = br.getChannel();
			byt = readPage();
		}catch(IOException e){
			System.out.println("Files not found.");
		}
		index = 8;
		if(byt!=null){
		    length = byt.getInt(0);
		    volume = byt.getInt(4);
		}
	}

	/**
	 * this method reads the page from a given file.
	 * @return the byte buffer of the page.
	 */
	@Override
	public ByteBuffer readPage() {
		// TODO Auto-generated method stub
		ByteBuffer buffer = ByteBuffer.allocate(4096);
		int total = 0;
		try {
			total = fc.read(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(total==-1) return null; // at the end of the file, return null.
		return buffer;
	}
	
	/**
	 * this method set the inital starting point of the reading.
	 * @param position the starting point of the file channel.
	 * @param index the starting point of the index.
	 */
	public boolean setPosition(long position, int index) {
		try {
			fc.position(position*4096);
			byt.position(0);
			fc.read(byt);
			length = byt.getInt(0);
			volume = byt.getInt(4);
			this.index = 8 + index * 4 * length;
		} catch (IOException e) {
			return false;
		}
		return true;
	}
	
	/**
	 * this method close the random access file.
	 */
	public void close(){
		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * this method returns the length of the tuple
	 * @return the length of the tuple.
	 */
	public int getLength(){
		return length;
	}

}

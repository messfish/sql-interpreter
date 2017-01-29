package cs4321.operator;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;


/** this is the class which is the superclass of all kinds of operators.
 * 
 * @author jz699 JUNCHEN ZHAN
 *
 */
public abstract class Operator implements TupleWriter{
    
	
	/**
	 * an abstract method, return the next tuple.
	 */
	public abstract Tuple getNextTuple();
	
	/**
	 * an abstract method, reset the pointer to the beginning of the table.
	 */
	public abstract void reset();
	
	/**
	 * for debugging, get all the tuples at once and put them in a file.
	 * @param s the location of the output files.
	 * @param index the index of the output file.
	 */
	public void dump(String s, String index) {
		// TODO Auto-generated method stub
		try{
		    File file = new File(s + index);
		    FileOutputStream fout = new FileOutputStream(file);
		    FileChannel fc = fout.getChannel();
		    ByteBuffer byt = null;
			while((byt=writePage())!=null){
				byt.limit(byt.capacity());
		        byt.position(0);
	            fc.write(byt);
			}
			fout.close();
	    }catch(Exception e){
			e.printStackTrace();;
		}
	}

	/**
     * fetch the tuples iteratively and write them on a page.
     * @return the byte buffer contains data.
     */
	@Override
	public ByteBuffer writePage() {
		// TODO Auto-generated method stub
		ByteBuffer buffer = ByteBuffer.allocate(4096);
		int index = 8, times = 0;
		Tuple tuple =  null;
		while((tuple=getNextTuple())!=null&&index+tuple.length()*8<=4096){
			buffer.putInt(0,tuple.length());
			for(int i=0;i<tuple.length();i++){
				buffer.putInt(index,tuple.getData(i));
				index += 4;
			}
			times++;
		}
		if(tuple!=null){
		    for(int i=0;i<tuple.length();i++){
			    buffer.putInt(index,tuple.getData(i));
			    index += 4;
		    }
		    times++;
		}
		if(times==0) return null; // no new tuples, no new byte buffers.
		while(index<4096){
			buffer.putInt(index,0);
			index += 4;
		}
		buffer.putInt(4,times);
		return buffer;
	}
	
}

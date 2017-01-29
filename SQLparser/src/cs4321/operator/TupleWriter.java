package cs4321.operator;

import java.nio.ByteBuffer;

/**
 * this interface tells how to write multiple tuples in one page
 * @author jz699 JUNCHEN ZHAN
 *
 */
public interface TupleWriter {

	/**
	 * this method writes the byte buffer and returns it. 
	 * @return the byte buffer written.
	 */
	ByteBuffer writePage();
	
}

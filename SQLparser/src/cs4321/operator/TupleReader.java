package cs4321.operator;

import java.nio.ByteBuffer;

/**
 * this interface reads as much tuples as they could
 * and put all of them on a page.
 * @author jz699 JUNCHEN ZHAN
 *
 */
public interface TupleReader {

	/**
	 * this method read data from a file channel and return a byte buffer.
	 * @return the byte buffer contains something read from a file channel.
	 */
    ByteBuffer readPage();
	
}

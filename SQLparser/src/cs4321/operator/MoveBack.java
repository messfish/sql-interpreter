package cs4321.operator;

/**
 * this is an interface that shall implement the method for 
 * reseting the index for the sort merge join.
 * @author jz699 JUNCHEN ZHAN
 *
 */
public interface MoveBack {

	/**
	 * an abstract method to get the next tuple back.
	 * @return the next tuple.
	 */
	Tuple getNextTuple();
	
	/** 
	 * an abstract method to set the point to move back.
	 */
	void anchor();
	
	/**
	 * an abstract method to reset the point to the specific position.
	 */
	void moveback();
	
	/**
	 * an abstract method to reset the point to the start of the file
	 */
	void reset();
	
}

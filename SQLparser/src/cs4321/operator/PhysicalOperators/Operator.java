package PhysicalOperators;

import TableElement.Tuple;

import java.util.HashMap;
import java.util.Map;

import Support.Mule;

/**
 * This class is the top level of the whole operator class.
 * Notice there are two main abstract methods that needs to be
 * implemented and they are the fundamentals of the database methods:
 * The first one is the getNextTuple() and the second one is 
 * reset(). 
 * @author messfish
 *
 */
public abstract class Operator {
	
	/**
	 * This abstract method is used to get the next valid tuple 
	 * from the table.
	 * @return the next valid tuple.
	 */
	public abstract Tuple getNextTuple();
	
	/**
	 * this abstract method is used to reset the pointer to the
	 * starting point of the table.
	 */
	public abstract void reset();
	
	/**
	 * This abstract method is used to get the schema of the table
	 * and store the result in a map, which has the string attribute
	 * as the key, a mule class which includes the index of the attribute
	 * and the data type of that attribute.
	 * @return a hash map includes the schema of the table.
	 */
	public abstract Map<String, Mule> getSchema();
	
}

package Support;

/**
 * This class stores the index of the schema and the 
 * data type of the data as integers.
 * @author messfish
 * 
 */
public class Mule{
	
	private int index;
	private int datatype;
	
	/**
	 * this is the getter method of the index.
	 * @return the index of the function.
	 */
	public int getIndex() {
		return index;
	}
	
	/**
	 * this is the getter method of the datatype.
	 * @return the data type.
	 */
	public int getDataType() {
		return datatype;
	}
	
	/**
	 * Constructor: this constructor takes the index and the datatype
	 * as the argument and store them in global variables, respectively.
	 * @param index
	 * @param datatype
	 */
	public Mule(int index, int datatype){
		this.index = index;
		this.datatype = datatype;
	}
	
}

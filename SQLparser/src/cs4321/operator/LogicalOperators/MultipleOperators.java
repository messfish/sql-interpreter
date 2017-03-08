package LogicalOperators;

import java.util.List;

/**
 * This class is the super class of the operators that have multiple
 * operators as their children. 
 * @author messfish
 *
 */
public abstract class MultipleOperators extends Operators{

	private List<Operators> childlist;
	
	/**
	 * Constructor: this constructor is mainly used to take 
	 * the child list as the argument and store that into a global variable.
	 * @param childlist a list of children operators.
	 */
	public MultipleOperators(List<Operators> childlist) {
		this.childlist = childlist;
	}
	
	/**
	 * this method is used to return the length of the list.
	 * @return the length of the child list.
	 */
	public int length() {
		return childlist.size();
	}
	
	/**
	 * this class is the getter method of a single child at a specific index.
	 * @param index the index that points to the child we need.
	 * @return the child operators at the given index.
	 */
	public Operators getChild(int index) {
		return childlist.get(index);
	}
	
}

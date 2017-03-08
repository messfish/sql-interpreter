package LogicalOperators;

/**
 * This class is the parent class that has a single children.
 * Also it is the child of the operators class.
 * @author messfish
 *
 */
public abstract class UnaryOperators extends Operators{

	private Operators child;
	
	/**
	 * Constructor: this constructor takes a operator from 
	 * the argument and stores it into the global variable.
	 * @param child the children of this operstor.
	 */
	public UnaryOperators(Operators child) {
		this.child = child;
	}
	
	/**
	 * this is the getter method of the children.
	 * @return the child of this operator.
	 */
	public Operators getChild() {
		return child;
	}
	
}

package logicalqueryplan;

import cs4321.operator.PhysicalPlanBuilder;

/**
 * This is an abstract class operator and the super class of 
 * all concrete logical operators.
 * @author jz699 JUNCHEN ZHAN
 *
 */
public abstract class Operators {

	private Operators child; // the child of this operator.
	
	/**
	 * Constructor: to let JoinOperators have a super constructor to use.
	 */
	public Operators(){}
	
	/**
	 * Constructor: pass the parameter of operator to the field.
	 * @param child
	 */
	public Operators(Operators child) {
		this.child = child;
	}
	
	/**
	 * Abstract method for accepting visitor.
	 * @param visitor visitor to be accepted.
	 * @param s the string that is used for indicating the tree depth.
	 * @param str the string that will be used for writing the file.
	 */
	public abstract void accept(PhysicalPlanBuilder visitor, String s, StringBuilder str);
	
	/**
	 * return the child operator.
	 * @return the operator of which is the child of the caller method.
	 */
	public Operators getChild(){
		return child;
	}
	
	/**
	 * this is the abstract value that will be used to print the whole plan.
	 * @param s the inital value that indicates the depth of a tree.
	 * @param str the string that will be used for writing the file.
	 */
	public abstract void print(String s, StringBuilder str);
	
}

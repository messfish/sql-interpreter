package logicalqueryplan;

import cs4321.operator.PhysicalPlanBuilder;

/**
 * This class deals with duplicate elimination by the time 
 * when the tuples are sorted.
 * @author jz699 JUNCHEN ZHAN
 *
 */
public class DuplicateEliminationOperators extends Operators {
		
	/**
	 * Constructor: pass the parameter of operator to the field.
	 * @param child
	 */
	public DuplicateEliminationOperators(Operators child) {
		super(child);
	}
	
	/**
	 * method for accepting visitor. just calls back visitor, logic of traversal
	 * will be handled in visitor method
	 * @param visitor visitor to be accepted.
	 * @param s the string that is used for indicating the tree depth.
	 * @param str the string that will be used for writing the file.
	 */
	@Override
	public void accept(PhysicalPlanBuilder visitor, String s, StringBuilder str) {
		// TODO Auto-generated method stub
		visitor.visit(this,s,str);
	}

	/**
	 * this is the print method that is used for generating the logical query plan.
	 * @param String the inital String that will be used for printing.
	 * @param str the string that will be used for writing the file.
	 */
	@Override
	public void print(String s, StringBuilder str) {
		// TODO Auto-generated method stub
		str.append(s + "DupElim").append("\n");
		this.getChild().print(s+"-",str);
	}

}

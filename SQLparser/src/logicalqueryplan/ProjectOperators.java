package logicalqueryplan;

import net.sf.jsqlparser.statement.select.PlainSelect;
import cs4321.operator.PhysicalPlanBuilder;

/**
 * this class handles the projection operator of a logical
 * plan.
 * @author jz699 JUNCHEN ZHAN
 *
 */
public class ProjectOperators extends Operators{
	
	private PlainSelect ps;
	
	/**
	 * Constructor: pass the parameter of operator to the field.
	 * @param child
	 */
	public ProjectOperators(Operators child, PlainSelect ps) {
		super(child);
		this.ps = ps;
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
		if(!ps.getSelectItems().get(0).toString().equals("*")){
			StringBuilder sb = new StringBuilder();
			sb.append(ps.getSelectItems().toString());
			str.append(s+"Project"+sb.toString()).append("\n");
			this.getChild().print(s+"-",str);
		}else this.getChild().print(s,str);
	}

}

package logicalqueryplan;

import net.sf.jsqlparser.statement.select.PlainSelect;

/**
 * this class deals with the order by language.
 * sort the tuples in the order presented by the Order By language.
 * @author jz699 JUNCHEN ZHAN
 *
 */
public class SortOperators extends Operators {
	
	private PlainSelect ps;
	
	/**
	 * Constructor: pass the parameter of operator to the field.
	 * @param child
	 */
	public SortOperators(Operators child, PlainSelect ps) {
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
	public void accept(LogicalQueryVisitor visitor, String s, StringBuilder str) {
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
		StringBuilder sb = new StringBuilder();
		if(ps.getOrderByElements()!=null){
			sb.append(ps.getOrderByElements().toString());
			str.append(s+"Sort"+sb.toString()).append("\n");
			this.getChild().print(s+"-",str);
		}else{
			str.append(s+"Sort"+"[]").append("\n");
			this.getChild().print(s+"-",str);
		}
	}
	
}

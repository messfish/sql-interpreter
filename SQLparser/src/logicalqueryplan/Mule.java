package logicalqueryplan;

import java.util.HashSet;
import java.util.Set;

/**
 * this class defines the data strucures for the union find.
 * @author jz699 JUNCHEN ZHAN
 *
 */
public class Mule{
	
	protected Set<String> list = new HashSet<>(); // the list of the string that is united.
	protected Integer lb; // the lower bound of this union.
	protected Integer up; // the upper bound of this union.
	protected Integer ec; // the equality constraints of this union.		
	
	/**
	 * return the set of columns.
	 * @return the set of columns.
	 */
	public Set<String> getList() {
		return list;
	}
	
}

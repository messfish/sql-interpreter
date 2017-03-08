package LogicalOperators;

/**
 * This class is the top class in the logical operator hierarchy.
 * It implements the visitors pattern which will be implemented
 * by a physical operator visitor.
 * 
 * @author messfish
 *
 */
public abstract class Operators {
	
	/**
	 * This is the abstract method of the accepting visitor.
	 * @param visit the visitors that handles the logic of the code.
	 */
	public abstract void accept(OperatorVisitor visit);
	
	/**
	 * This is mainly used for debugging, it will store the structure
	 * of the tree in the string builder. 
	 * @param s indicates the level of the tree.
	 * @param sb the string that stores the tree structure.
	 */
	public abstract void print(String s, StringBuilder sb);
	
}

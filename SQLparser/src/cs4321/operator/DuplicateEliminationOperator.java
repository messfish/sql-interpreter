package cs4321.operator;

/**
 * this is the class which handles the distinct operator
 * when the tuples are sorted. 
 * @author jz699 JUNCHEN ZHAN
 *
 */
public class DuplicateEliminationOperator extends Operator{

	private Operator exs; // the external sort operator.
	private Tuple tup; // the tuple serve as a temporary one.
	
	/**
	 * constructor: construct the distinct operator 
	 * which the tuples are sorted.
	 * @param sop the operator which tuples are sorted in external way.
	 */
	public DuplicateEliminationOperator(Operator sop) {
		exs = sop;
	}

    /**
     * method that gets the next tuple.
     * @return the next tuple.
     */
	@Override
	public Tuple getNextTuple() {
		Tuple tuple = exs.getNextTuple();
		if(tuple==null) return null;
		while(tup!=null&&tuple!=null){
			if(tup.equals(tuple)) tuple = exs.getNextTuple();
			else break;
		}
		tup = tuple;
		return tuple;
	}

	/**
	 * method that reset the operator.
	 */
	@Override
	public void reset() {
		tup = null;
		exs.reset();
	}
	
}

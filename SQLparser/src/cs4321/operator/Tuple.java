package cs4321.operator;

/**
 * The self-defined Tuple class which contains a list of integers as data.
 * @author jz699 JUNCHEN ZHAN
 *
 */
public class Tuple {
 
	private int[] list; // they array contains the data, which is a long integer.

	/**
	 * Constructor: Convert the stream of string into an array.
	 * for the assignment, we convert them into Integers since 
	 * the only data type is Integers.
	 * @param s a string stream of the tuple.
	 */
	public Tuple(String s){
		String[] temp = s.split(",");
		list = new int[temp.length];
		for(int i=0;i<temp.length;i++)
			list[i] = Integer.parseInt(temp[i]);
	}
	
	/**
	 * Constructor: set a new empty array with a fixed length.
	 * @param length the length of the tuple.
	 */
	public Tuple(int length){
		list = new int[length];
	}
	
	/**
	 * Constructor: set a tuple with a array.
	 * @param the array which is going to be included in the tuple.
	 */
	public Tuple(int[] list){
		this.list = list;
	}
	
	/**
	 * return the data from the index
	 * @param the index of the scheme
	 * @return the value referenced from the index.
	 */
	public int getData(int index) {
		return list[index];
	}
	
	/**
	 * For debugging.
	 * print out the tuple, there is a blank between each value.
	 */
	public void print(){
		for(int i=0;i<list.length;i++){
			System.out.print(list[i]);
			System.out.print(" ");
		}
		System.out.println();
	}
	
	/**
	 * set the index of the tuple to the desired value.
	 * @param index the index of the array you want to change.
	 * @param val the value to want to switch to.
	 */
	public void setData(int index, int val){
		list[index] = val;
	}
	
	/**
	 * get the length of the list
	 * @return the length of the list.
	 */
	public int length(){
		return list.length;
	}
	
	/**
	 * check whether this tuple equals with that tuple.
	 * @param tuple the tuple to be compared.
	 * @return this tuple is equal to that tuple.
	 */
	public boolean equals(Tuple tuple) {
		if(this==tuple) return true;
		if(this.list.length!=tuple.list.length) return false;
		for(int i=0;i<tuple.list.length;i++){
			if(this.list[i]!=tuple.list[i])
				return false;
		}
		return true;
	}
	
	/**
	 * convert the list of data to string.
	 * @return the converted string.
	 */
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(int i=0;i<this.list.length;i++){
			sb.append(list[i]);
			sb.append(",");
		}
		return sb.deleteCharAt(sb.length()-1).toString();
	}
}

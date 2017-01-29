package cs4321.operator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import cs4321.project2.Interpreter;

/**
 * this class performs the external sort based on the order of columns chosen. 
 * Get all the tuples from an operator and put them in temporary file folder.
 * Sort the folder by using the external sort algorithm. Put the result in
 * the temporary file folder for the sake of getNextTuple().
 * @author jz699 JUNCHEN ZHAN
 *
 */
public class EXSortOperator extends Operator implements MoveBack{

    private int pages; // the number of buffer pages used.
    private int nums; // the number of pages that will be used.
    private Map<String, Integer> location; // the location map.
    private String store = Interpreter.getTemp(); 
    // the file location to sort those temporary pages.
    private String[] array; // the array of strings used for sorting.
    private Operator op; // the operator that will take the external sort.
    private int sorttimes; // takes down the times perform an external sort.
    private int index; // takes down the index of this external sort.
    private ScanOperator sco; // the scan operator. 
    
	/**
	 * constructor: get all the materials needed for external sorting.
	 * @param op the operator that will be sorted.
	 * @param str the list of sort columns.
	 * @param location the map of which columns the sort will perform.
	 * @param pages the number of buffer pages.
	 * @param index. Use these to avoid duplicate file location 
	 * when performing an SMJ.
	 */
	public EXSortOperator(Operator op, String[] array, Map<String, Integer> location, int pages, int index){
		this.location = location;
		this.pages = pages;
		this.array = array;
		this.op = op;
		this.index = index;
		try{
		    ByteBuffer byt = null;
			while((byt=write())!=null){
		        File file = new File(store + index + "," + nums);
		        FileOutputStream fout = new FileOutputStream(file);
		        FileChannel fc = fout.getChannel();
		        byt.position(0);
	            fc.write(byt);
				fout.close();
				nums++;
			}
	    }catch(IOException e){
			System.out.println("An exception occurs!");
		}
		if(nums!=0){
		    int result = externalSort();
		    File file = new File(store + index + "," + result);
		    sco = new ScanOperator(file);
		}
	}
	
	/**
	 * call the anchor() method from the Scan Operator.
	 */
	@Override
	public void anchor(){
		sco.anchor();
	}
	
	/**
	 * call the moveback() method from the Scan Operator.
	 */
	@Override
	public void moveback(){
		sco.moveback();
	}
	
	/**
	 * get the next tuple avaiable. If not, return null.
	 * @return the next tuple.
	 */
	@Override
	public Tuple getNextTuple() {
		// TODO Auto-generated method stub
		if(nums==0) return null;
		return sco.getNextTuple();
	}

	/**
	 * set the pointer to the start of the file.
	 */
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		if(nums!=0) sco.reset();
	}
	
	/**
	 * this method is used to perform external sort.
	 * if this is the first time to sort, perform the in memory sort.
	 * if it is not the first time, do the merge sort in pages-1 slots.
	 * @return the index that indicates the location of the result file.
	 */
	private int externalSort(){
		int newindex = nums, result = newindex;
		boolean isDone = false; // in case there are only one page left.
		// there are only one page left, the external sort is finished.
		while(nums>1||!isDone){
			if(sorttimes==0){
				int num = 0, number = 0;
				while(num<nums){
					int i = 0;
					List<Tuple> temp = new ArrayList<>();
					while(num<nums&&i<pages){
						File file = new File(store + index + "," + num);
						ScanOperator scan = new ScanOperator(file);
						Tuple tuple = null;
						while((tuple=scan.getNextTuple())!=null)
							temp.add(tuple);
						i++;
						num++;
					}
					Collections.sort(temp,new Comparator<Tuple>(){
						@Override
						public int compare(Tuple tup1, Tuple tup2) {
							// TODO Auto-generated method stub
							// sort tuples from the order by language first.
							for(int i=0;i<array.length;i++){
								int index = location.get(array[i]);
								if(tup1.getData(index)>tup2.getData(index)) return 1;
								else if(tup1.getData(index)<tup2.getData(index)) return -1;
							}
							// sort tuples by the order of the tuples.
							for(int i=0;i<location.size()&&i<tup1.length();i++){
								if(tup1.getData(i)>tup2.getData(i)) return 1;
								else if(tup1.getData(i)<tup2.getData(i)) return -1;
							}
							return 0;
						}	
					});
					try{
					    File file = new File(store + index + "," + newindex);
					    FileOutputStream fout = new FileOutputStream(file);
					    FileChannel fc = fout.getChannel();
					    ByteBuffer byt = null;
					    int point = 0;
						while((byt=write(temp,point))!=null){
					        byt.position(0);
				            fc.write(byt);
				            point += byt.getInt(4);
						}
						fout.close();
				    }catch(Exception e){
						System.out.println("An exception occurs!");
					}
					newindex++;
					number++;
				}
				nums = number;
				isDone = true;
				sorttimes++;
			}else{
				int num = 0, number = 0;
				while(num<nums){
					int i = 0, dummy = 8;
					ScanOperator[] scanarray = new ScanOperator[pages-1];
					Tuple[] temp = new Tuple[pages-1];
					while(num<nums&&i<temp.length){
						File file = new File(store + index + "," + result);
						ScanOperator scan = new ScanOperator(file);
						scanarray[i] = scan;
						temp[i] = scan.getNextTuple();
						i++;
						num++;
						result++;
					}
					boolean isOver = false;
					try{
						int times = 0;
						File file = new File(store + index + "," + newindex);
						FileOutputStream fout = new FileOutputStream(file);
				    	FileChannel fc = fout.getChannel();
				    	ByteBuffer byt = ByteBuffer.allocate(4096);
				    	ByteBuffer buffer = ByteBuffer.allocate(4096);
						while(!isOver){
							int target = 0;
							while(target<temp.length&&temp[target]==null)
								target++;
						    	if(target==temp.length){
						    		buffer.putInt(4,times);
						    		while(dummy<4096){
						    			buffer.putInt(dummy,0);
						    			dummy += 4;
						    		}
						    		byt = buffer;
						    		byt.position(0);
						    		fc.write(byt);
									isOver = true;
						    	}else{
						    		for(int j=target+1;j<i;j++){
						    			if(temp[j]!=null&&compare(temp[target],temp[j])>0)
						    				target = j;
								}
								Tuple tuple = temp[target];
								buffer.putInt(0,tuple.length());
								if(dummy+tuple.length()*4<=4096){
									for(int x=0;x<tuple.length();x++){
										buffer.putInt(dummy,tuple.getData(x));
										dummy += 4;
									}
									times++;
								}
								else{
									buffer.putInt(4,times);
									while(dummy<4096){
										buffer.putInt(dummy,0);
										dummy += 4;
									}
									byt = buffer;
									byt.position(0);
									fc.write(byt);
									buffer = ByteBuffer.allocate(4096);
									dummy = 8;
									times = 0;
									buffer.putInt(0,tuple.length());
									for(int x=0;x<tuple.length();x++){
										buffer.putInt(dummy,tuple.getData(x));
										dummy += 4;
									}
									times++;
								}
								temp[target] = scanarray[target].getNextTuple();
							}						
						}
						fout.close();
					newindex++;
					number++;
					}catch(Exception e){
						System.out.println("An exception occuring!");
					}
			    }
				nums = number;
				sorttimes++;
			}
		}
		return newindex-1;
	}
	
	/**
	 * compare the two tuples based on the order of strings.
	 * @param tup1 the one indicates the target.
	 * @param tup2 the one where the tuples are compared to.
	 * @return an integer, negative means tuple1 is smaller than tuple2
	 * positive means tuple1 is bigger than tuple2.
	 * zero means they are equal.
	 */
	private int compare(Tuple tup1, Tuple tup2){
		for(int i=0;i<array.length;i++){
			int index = location.get(array[i]);
			if(tup1.getData(index)>tup2.getData(index)) return 1;
			else if(tup1.getData(index)<tup2.getData(index)) return -1;
		}
		// sort tuples by the order of the tuples.
		for(int i=0;i<location.size()&&i<tup1.length();i++){
			if(tup1.getData(i)>tup2.getData(i)) return 1;
			else if(tup1.getData(i)<tup2.getData(i)) return -1;
		}
		return 0;
	}
	
	/**
     * fetch the tuples iteratively and write them on a page.
     * @param temp the list of tuples to write.
     * @param point the pointer to the index.
     * @return the byte buffer contains data.
     */
	public ByteBuffer write(List<Tuple> temp, int point){
		ByteBuffer buffer = ByteBuffer.allocate(4096);
		int index = 8, times = 0;
		while(point<temp.size()&&index+temp.get(point).length()*4<=4096){
			Tuple tuple = temp.get(point);
			buffer.putInt(0,tuple.length());
			for(int i=0;i<tuple.length();i++){
				buffer.putInt(index,tuple.getData(i));
				index += 4;
			}
			times++;
			point++;
		}
		if(times==0) return null; // no new tuples, no new byte buffers.
		while(index<4096){
			buffer.putInt(index,0);
			index += 4;
		}
		buffer.putInt(4,times);
		return buffer;
	}

	/**
     * fetch the tuples iteratively and write them on a page.
     * @return the byte buffer contains data.
     */
	public ByteBuffer write() {
		// TODO Auto-generated method stub
		ByteBuffer buffer = ByteBuffer.allocate(4096);
		int index = 8, times = 0;
		Tuple tuple =  null;
		while((tuple=op.getNextTuple())!=null&&index+tuple.length()*8<=4096){
			buffer.putInt(0,tuple.length());
			for(int i=0;i<tuple.length();i++){
				buffer.putInt(index,tuple.getData(i));
				index += 4;
			}
			times++;
		}
		if(tuple!=null){
			for(int i=0;i<tuple.length();i++){
				buffer.putInt(index,tuple.getData(i));
				index += 4;
			}
		    times++;
	    }
		if(times==0) return null; // no new tuples, no new byte buffers.
		while(index<4096){
			buffer.putInt(index,0);
			index += 4;
		}
		buffer.putInt(4,times);
		return buffer;
	}

}

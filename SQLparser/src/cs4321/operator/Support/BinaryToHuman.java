package Support;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import SmallSQLServer.Main;

/**
 * As the name suggests, this class is mainly used to deal with converting
 * binary files to the human readable file. Notice in that file, one tuple
 * will consume one single line. And here is how I store the list of data:
 * regardless of the type of the data in the menu, simply store that as a
 * string variable. for each string variable. Append a number that indicates
 * the length of the string and a slash, followed by the string itself.
 * Also we need to transfer the date and time data into their specific 
 * format, which will be integers.
 * @author messfish
 *
 */
public class BinaryToHuman {

	private static final int NUM_OF_BYTES = 16384;
	private TimeConversion convert = new TimeConversion();
	
	/**
	 * this method is used to convert the binary file into a human readable
	 * file. Notice the human readable file should follow the format that 
	 * is written in the definition of the class.
	 * @param binary the file in the binary form.
	 * @title the name of the table.
	 * @return the file in the human readable form.
	 */
	public File convert(File binary, String title) {
		File result = new File(Main.getTest() + "/conversiontest/" + title);
		FileInputStream input = null;
		List<Integer> list = new ArrayList<>();
		try {
			input = new FileInputStream(binary);
			FileChannel fc = input.getChannel();
			ByteBuffer buffer = ByteBuffer.allocate(NUM_OF_BYTES);
			fc.read(buffer);
			StringBuilder sb = new StringBuilder();
			int times = buffer.get(0), index = 1;
			for(int i=0;i<times;i++) {
				list.add(-1);
				index = writeLine(buffer,list,sb,index);
			}
			int[] typelist = new int[list.size()];
			for(int i=0;i<typelist.length;i++)
				typelist[i] = list.get(i);
			readPage(fc, sb, typelist);
			BufferedWriter write = new BufferedWriter(new FileWriter(result));
			write.write(sb.toString());
			write.close();
		} catch (Exception e) {
			System.out.println("Could not find the binary file specified!");
		}
		return result;
	}

	/**
	 * this helper function is used to deal with reading a page from a 
	 * binary file and store the content into the string builder. Notice 
	 * the content in the string need to follow the format defined in 
	 * the declaration of the class above. 
	 * @param fc the file channel for the list.
	 * @param sb the string builder that stores the tuples.
	 * @param typelist this is a list of types that store integers.
	 */
	private void readPage(FileChannel fc, StringBuilder sb, int[] typelist) {
		ByteBuffer buffer = ByteBuffer.allocate(NUM_OF_BYTES);
		int length = 0;
		try {
			/* notice the read() return -1 when there are no bytes left. */
			while(length!=-1) {
				length = fc.read(buffer);
				/* notice the first 4 bytes stores the number of tuples
				 * available in the buffer page. */
				int size = buffer.getInt(0), index = 4;
				for(int i=0;i<size;i++) 
					/* notice there are is a byte to show whether the tuple is 
					 * valid, so we need to increase the value. */
					index = writeTuple(buffer, index + 1, sb, typelist);
				/* do not forget to refresh the byte buffer! */
				buffer = ByteBuffer.allocate(NUM_OF_BYTES);
			}
		}catch(Exception e) {
			System.out.println("There is something wrong at the file channel!");
		}
	}
	
	/**
	 * this helper method is used to write a single tuple. notice the tuple
	 * must follow the format defined in the definition of the class.
	 * Also, 1 is the numeric value, 2 is a string value, 3 is 
	 * the date value and 4 is the time value.
	 * @param buffer the byte buffer used for reading.
	 * @param index the starting point of the byte buffer.
	 * @param sb the string builder that stores the tuple.
	 * @param typelist a list of types represented as integers.
	 * @return the ending point of the index.
	 */
	private int writeTuple(ByteBuffer buffer, int index,
						   StringBuilder sb, int[] typelist) {
		for(int i=0;i<typelist.length;i++) {
			/* this indicates the number is the order of individual table.
			 * In this case, we need to skip this number. */
			if(typelist[i]==-1) index += 4;
			else if(typelist[i]==1) {
				long data = buffer.getLong(index);
				String temp = String.valueOf(data);
				sb.append(temp.length()).append("/").append(temp+" ");
				index += 8;
			}else if(typelist[i]==2) {
				/* notice the first byte stores the length of the string. */
				int length = buffer.get(index);
				index++;
				StringBuilder temp = new StringBuilder();
				for(int j=0;j<length;j++) {
					char c = buffer.getChar(index);
					temp.append(c);
					index++;
				}
				sb.append(temp.length()).append("/").append(temp+" ");
			}else if(typelist[i]==3) {
				double data = buffer.getDouble(index);
				String temp = convert.fromNumberToDate(data);
				sb.append(temp.length()).append("/").append(temp+" ");
				index += 8;
			}else if(typelist[i]==4) {
				double data = buffer.getDouble(index);
				String temp = convert.fromNumberToTime(data);
				sb.append(temp.length()).append("/").append(temp+" ");
				index += 8;
			}else if(typelist[i]==5) {
				double data = buffer.getDouble(index);
				String temp = String.valueOf(data);
				sb.append(temp.length()).append("/").append(temp+" ");
				index += 8;
			}
			/* this is mainly used for debugging. */
			else {
				System.out.println("You get an invalid type!");
			}
		}
		/* at last, do not forget to start a new line. */
		sb.append("\n");
		return index;
	}
	
	/**
	 * This method writes a line that contains the schema of the table.
	 * @param buffer the buffer page that serves as the source page.
	 * @param list the list that stores the type of the data.
	 * @param sb the string builder that stores the written line.
	 * @param index the index that points where the buffer page should refer.
	 * @return the new index of the buffer page.
	 */
	private int writeLine(ByteBuffer buffer, List<Integer> list, 
					      StringBuilder sb, int index) {
		int size = buffer.get(index);
		index++;
		StringBuilder build = new StringBuilder();
		int length = buffer.get(index);
		index++;
		for(int j=0;j<length;j++) {
			char c = (char) buffer.get(index);
			System.out.println(c);
			build.append(c);
			index++;
		}
		sb.append(build).append(" ");
		for(int j=0;j<size;j++) {
			StringBuilder sb1 = new StringBuilder();
			int length1 = buffer.get(index);
			index++;
			for(int k=0;k<length1;k++) {
				char c = (char) buffer.get(index);
				System.out.println(c);
				sb1.append(c);
				index++;
			}
			sb.append(sb1).append(" ");
			int type = buffer.get(index);
			list.add(type);
			index++;
		}
		sb.append("\n");
		return index;
	}
	
}

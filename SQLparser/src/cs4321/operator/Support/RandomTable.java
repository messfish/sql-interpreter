package Support;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import SmallSQLServer.Main;

/**
 * This class is mainly used for debugging.
 * It generate a human readable file that could be used for testing. 
 * In this file all the data in it are randomized.
 * @author messfish
 *
 */
public class RandomTable {

	int limit; // the number of tuples you want for the file.
	
	/**
	 * Constructor: this constructor is mainly used to set the 
	 * limit of the data file.
	 * @param limit the number of tuples in the file.
	 */
	public RandomTable(int limit) {
		this.limit = limit;
	}
	
	/**
	 * This method is used to write a file that generates random values.
	 * @param index this integer is used to mark the file.
	 * @return a file with random values.
	 */
	public File generate(int index) {
		File result = new File(Main.getTest() + "/conversiontest/file" + index);
		int size = 0;
		try {
			StringBuilder sb = new StringBuilder();
			sb.append("Test ").append("Ta ").append("Tb ")
			  .append("Tc ").append("Td ").append("\n");
			BufferedWriter write = new BufferedWriter(new FileWriter(result));
			while(size<limit) {
				long data = (long)(Math.random()*1000000);
				String str = String.valueOf(data);
				sb.append(str.length()).append("/").append(str + " ");
				double data1 = Math.random() * 1000000;
				String str1 = String.valueOf(data1);
				sb.append(str1.length()).append("/").append(str1 + " ");
				generateRandomDate(sb);
				generateRandomTime(sb);
				sb.append("\n");
				size++;
			}
			write.write(sb.toString());
			write.close();
		} catch (IOException e) {
			System.out.println("Cannot find the file specified!");
		}
		return result;
	}
	
	/**
	 * This is the helper function that writes the date with a random
	 * value. Notice it must falls into the defined range.
	 * @param sb the string that stores the randomly generated date.
	 */
	private void generateRandomDate(StringBuilder sb) {
		int year = (int)(Math.random()*10000);
		int month = (int)(Math.random()*12) + 1;
		int date = (int)(Math.random()*28) + 1;
		String str = String.format("%04d", year) + "/" +
					 String.format("%02d", month) + "/" +
					 String.format("%02d", date);
		sb.append(str.length()).append("/").append(str + " ");
	}
	
	/**
	 * This is the helper function that writes the time with a random
	 * value. Notice it must falls into the defined range.
	 * @param sb the string that stores the randomly generated date.
	 */
	private void generateRandomTime(StringBuilder sb) {
		int hour = (int)(Math.random()*24);
		int minute = (int)(Math.random()*60);
		int second = (int)(Math.random()*60);
		String str = String.format("%02d", hour) + ":" +
					 String.format("%02d", minute) + ":" +
					 String.format("%02d", second);
		sb.append(str.length()).append("/").append(str + " ");
	}
	
}

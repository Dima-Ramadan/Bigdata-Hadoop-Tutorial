/* This is the reducer for HistoricalFires job */

/* The main aim of this class is to find the mean of fire area and fore brightness  per month for the location entered 
 * by the user in the prompt */
/* This class will take the output of HistoricalFiresDateMapper (Text, Text) ==> [(YYYY-MM,Location), (FireArea, FireBrightness)] 
and will emit [(date[YYYY - MM],Location), (AverageArea, Average FireBrightness)] */


package org.myorg;
// Importing the libraries that contain the classes and methods needed in the Reducer class
import java.io.IOException;
import java.util.Iterator;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HistoricalWildfiresReducer extends Reducer<Text, Text, Text, Text>
{
	 //create String arraylist called ValueList
	 ArrayList<String> ValuesList = new ArrayList<String>();
	 //Declare the value name that will be emitted out of the reducer
	 Text AreaFireStrength = new Text();
	 @Override
	 public void reduce(Text key, Iterable<Text> values, Context context)
	 throws IOException, InterruptedException
	 { 
		 //create a double variable called AreaSum and initialise it with 0.0
		 //Create a double variable called FireBrightSum and initialise it with 0.0
		double AreaSum = 0.0;
		double FireBrightSum = 0.0;
		int count = 0 ; //Create a integer variable called count and initialise it with 0
		 
		//Create Iterator object that iterates through the Text values and save the result to Text iterator 
		Iterator<Text> iterator = values.iterator();
		//check if there is at least one item left to iterate over using .hasNext() method.
		while (iterator.hasNext()) {
			//iterate through the values in order -not randomly- using next() method and extract the value in the iterator. 
			Text value = iterator.next();
			// split the Text value (area, brightness) on comma and save the result in list Line
			String []  Line  = value.toString().split(",");
			//trim the white space from in the Line array and converts it to double data type
			double Area = Double.parseDouble(Line[0].trim());
			double FireBright = Double.parseDouble(Line[1].trim());
			//Sum the the area and fireBrightness for each the value associated with same key 
			AreaSum +=  Area;
			FireBrightSum += FireBright;
			//increase the count by one
			count = count + 1;
		}
		// find the mean values of area and fire brightness for each key 
			double meanArea = AreaSum / count; 
			double meanFireBright = FireBrightSum / count;
			
			// converts the resultant means to strings and concatenate them together we can emit them as a value 
			String AreaFireBrightness = String.valueOf(meanArea)+ "," +String.valueOf(meanFireBright);
			//set AreaFireStrength hadoop variable to the value of java base variable AreaFireBrightness
			AreaFireStrength.set(AreaFireBrightness);
		context.write(key, AreaFireStrength); //[Text, Text]
	 } }

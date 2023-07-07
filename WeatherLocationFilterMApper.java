package org.myorg;

//Importing the libraries that will be used in Mapper Class 
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//extend WeatherLocationFilterMApper from Mapper Class in hadoop 
// the method will take LongWritable and Text as inputs (data file -1st and second arguments) and 
//will emit Text key and Text Value (3rd and 4th arguments).

public class WeatherLocationFilterMApper extends Mapper<LongWritable, Text, Text, Text>{
	
	//Declare hadoop's variables that will be emitted from the class 
	private Text LocationName = new Text(); // the filtered location and will be the key
	private Text VarReturn = new Text(); //(Date,TempMax) will be the value.
	 
	 /*@override is used to change the original behaviour of the method “map” in the parent Class “Mapper” */
	 @Override
	 
	 // the map method has three parameters,
	 //(the inputs and the context that allow the data transition between mapreduce components  
	 public void map(LongWritable key, Text value, Context context)
	 throws IOException, InterruptedException
	 { 
		 // the configurations used to extract the value of the 5th argument [region] from the command line
		 // you can choose from (NSW, NT, QL, SA, TA, VI, and WA) (case insensitive)
		 Configuration conf = context.getConfiguration();
		// Save the value of the conf. in Location Entry
		 String LocationEntry= conf.get("LocationFilter");
		 //set the LocationName variable (hadoop variable) to LocationEntry(base java) because it will be emitted from the mapper
		 LocationName.set(LocationEntry);
		
		 //Split the values of each line on each comma and save it in line array 
		 String[] line = value.toString().split(","); 
		 
		 //extract the value in the first index (date) in line array.
		 String Date = line[0];
		
		 //extract the Region(Location) value which is in index 1
		 String Location = line[1];

		 //extract the parameter values in index 2 of each line (we will use this to filter to Temperature only)
		 String param = line[2];

		 //Extract the max temperature in each day in index 5 (we are interested in this because it serves the research question)
		 String TempMax = line[5];
 
		 /* The first conditional statement compares between the Region Value in index 1, and the user entry in command line,
		  it will only choose the region the user enters ( NOTE: the comparison is not case sensitive)*/
		 
		 /* The second if statement will choose only Temperature values (neglect humidity, SolarRadiation ..etc)  the comparison here 
		  is case sensitive*/

		 if (Location.equalsIgnoreCase(LocationEntry) )
		 {
			 if (param.equals("Temperature")){
				 
				 // if all the condition are met, it will join the date and TempMax (as strings) 
				 String varReturn = Date + "," + TempMax; 
				 
				 /*Set the Text Variable VarReturn (hadoop variable) to the value of varReturn (base JAVA) because it will be emitted.*/
				 VarReturn.set(varReturn);
				 //emit Text key and Text Value 
				 context.write(LocationName, VarReturn );
				 }
		 }
	 }
}

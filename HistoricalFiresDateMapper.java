/* This is the second Class in the HostoricalWeather.txt multistep mapreduce job chain. */
/* The main aim of this class is to emit the date (YYYY-mm) and region as a key and (Area, fireBrightnes) tuple as a value.
 */
/* The Class will take the output of HistoricalFiresMapper as an input (Text , Text), 
 * and will emit [(date,Location), (Area, FireBrightness)] [Text,Text] */

package org.myorg;
//Importing the libraries that will be used in Mapper Class 
import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;

public class HistoricalFiresDateMapper extends Mapper<Text, Text, Text, Text>{	
	//Declare hadoop variables that will be emitted from the class 	
	private Text DateLocation = new Text();
	private Text AreaFBright= new Text();
	
	 /*@override is used to change the original behaviour of the method “map” in the parent
	 Class “Mapper” */
	 @Override
	 
	 public void map(Text key, Text value, Context context)
	 throws IOException, InterruptedException
	 { 
		 //Split the values in each line on each comma
		 String[] DateAreaBrightness= value.toString().split(",");
		 //Extract the date variable mm/dd/yyyy
		 String DateEx = DateAreaBrightness[0];
		 //split the date on slash
		 String[] DateSplit = DateEx.split("/");
		 //extract the year and month of date and save it in the new format YYYY-mm
		 String DateYM = DateSplit[2] + "-" + DateSplit[0]; 
	
		 //convert the data type og the location to string so we can concatenate it with other strings
		 String Location = key.toString();
		 
		 //concatenate the data year-month and region and set the hadoop variable DateLocation to the resulting string
		 String DateLoc = DateYM + "," +  Location;
		 DateLocation.set(DateLoc);
		 
		 //join the area anf the fire brightnes into one string 
		 String AreaBright = DateAreaBrightness[1] + "," + DateAreaBrightness[2];
		 //set the hadoop variable AreaFBright to the value of AreaBright
		 AreaFBright.set(AreaBright);
				
		 context.write(DateLocation, AreaFBright); //[Text, Text]
	 }	 
}
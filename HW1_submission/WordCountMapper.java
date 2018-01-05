import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class WordCountMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	{
		
		int numberOfChapters=Integer.parseInt(context.getConfiguration().get("numberOfChapters"));       
		String cleanLine=getCleanString(value.toString());
		StringTokenizer itr = new StringTokenizer(cleanLine); 
		Text word = new Text();
		//context.write(new Text(cleanLine),new LongWritable(1));
		
		
		int chapterNumber=0;
		int hasRoman=0;
		int iterationNumber=1;
		String firstLine="";
		for(String line:cleanLine.split("\n")){
				if(iterationNumber==1)
				{
			    String words[]=line.split(" ");
			    firstLine=line;
				String finalRomanString=words[1];
				chapterNumber=romanToDecimal(finalRomanString);
				if(chapterNumber!=0)
				hasRoman=1;
				break;
				}
		}
		
		
		if(hasRoman==1 && chapterNumber!=numberOfChapters)
		{
		 while (itr.hasMoreTokens()) {    
			 word.set(itr.nextToken().trim());               
			 context.write(word, new LongWritable(chapterNumber));  
			 }	
		}
		if(chapterNumber==numberOfChapters)
		{
			for(String line:cleanLine.split("\n"))
			{
				if(line.contains("end"))
					break;
				StringTokenizer newItr=new StringTokenizer(line);
				while (newItr.hasMoreTokens()) {    
					 word.set(newItr.nextToken().trim());               
					 context.write(word, new LongWritable(chapterNumber));  
					 }
					 
			}
		}         	 
		
	}
	
	//Methods to convert from roman to decimal
	public static int romanToDecimal(String romanNumber) {
	    int decimal = 0;
	    int lastNumber = 0;
	    String romanNumeral = romanNumber.toUpperCase();
	         /* operation to be performed on upper cases even if user enters roman values in lower case chars */
	    for (int x = romanNumeral.length() - 1; x >= 0 ; x--) {
	        char convertToDecimal = romanNumeral.charAt(x);

	        switch (convertToDecimal) {
	            case 'M':
	                decimal = processDecimal(1000, lastNumber, decimal);
	                lastNumber = 1000;
	                break;

	            case 'D':
	                decimal = processDecimal(500, lastNumber, decimal);
	                lastNumber = 500;
	                break;

	            case 'C':
	                decimal = processDecimal(100, lastNumber, decimal);
	                lastNumber = 100;
	                break;

	            case 'L':
	                decimal = processDecimal(50, lastNumber, decimal);
	                lastNumber = 50;
	                break;

	            case 'X':
	                decimal = processDecimal(10, lastNumber, decimal);
	                lastNumber = 10;
	                break;

	            case 'V':
	                decimal = processDecimal(5, lastNumber, decimal);
	                lastNumber = 5;
	                break;

	            case 'I':
	                decimal = processDecimal(1, lastNumber, decimal);
	                lastNumber = 1;
	                break;
	        }
	    }
	    return decimal;
	}

	public static int processDecimal(int decimal, int lastNumber, int lastDecimal) {
	    if (lastNumber > decimal) {
	        return lastDecimal - decimal;
	    } else {
	        return lastDecimal + decimal;
	    }
	}
	public static String getCleanString(String inputString)
	{
		String cleanLine=inputString;
		String tokens = "[_|$#<>\\^=\\[\\]\\\\\\,;,.\\-:()?!\"']";
		cleanLine = cleanLine.toLowerCase().replaceAll(tokens, " "); 
		cleanLine= cleanLine.replaceAll("[\\pP\\p{Punct}\\d+]", " "); 
		cleanLine= cleanLine.toLowerCase();
		
		cleanLine= cleanLine.replaceAll(" the ", " "); 
		cleanLine= cleanLine.replaceAll("\nthe ", "\n");
		cleanLine= cleanLine.replaceAll(" the\n", "\n");
		
		cleanLine= cleanLine.replaceAll(" a ", " ");
		cleanLine= cleanLine.replaceAll("\na ", "\n");
		cleanLine= cleanLine.replaceAll(" a\n", "\n");
		
		cleanLine= cleanLine.replaceAll(" an ", " ");
		cleanLine= cleanLine.replaceAll("\nan ", "\n");
		cleanLine= cleanLine.replaceAll(" an\n", "\n");
		
		cleanLine=cleanLine.replaceAll(" s ", "");
		cleanLine= cleanLine.replaceAll("\ns ", "\n");
		cleanLine= cleanLine.replaceAll(" s\n", "\n");
		
		cleanLine=cleanLine.replaceAll(" t ", "");
		cleanLine= cleanLine.replaceAll("\nt ", "\n");
		cleanLine= cleanLine.replaceAll(" t\n", "\n");
		
		cleanLine=cleanLine.replaceAll(" re ", "");
		cleanLine= cleanLine.replaceAll("\nre ", "\n");
		cleanLine= cleanLine.replaceAll(" re\n", "\n");
		return cleanLine;
	}

}

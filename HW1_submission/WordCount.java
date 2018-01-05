import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;



public class WordCount extends Configured{

	public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException
	{
		////Part for finding number of chapters
		String fileName = args[0];
		System.out.println(fileName);
        // This will reference one line at a time
        String line = null;
        int numberOfChapters=0;
        
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = 
                new FileReader(fileName);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = 
                new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
            	//System.out.println(line);
                if(line.contains("CHAPTER"))
                	numberOfChapters+=1;
                //if(line.contains("THE END"))
                //	break;
            }   

            // Always close files.
            bufferedReader.close();         
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                "Unable to open file '" + 
                fileName + "'");                
        }
        catch(IOException ex) {
            System.out.println(
                "Error reading file '" 
                + fileName + "'");                  
        }
        
        ///Part for setting variables
		Configuration conf = new Configuration(true);
		conf.set("textinputformat.record.delimiter","CHAPTER");
		conf.set("numberOfChapters",Integer.toString(numberOfChapters));
		Job job=new Job(conf);
		job.setInputFormatClass(TextInputFormat.class);
		job.setJarByClass(WordCount.class);
		
		
		TextInputFormat.addInputPath(job, new Path(args[0]));
		//job.getConfiguration().setInt(
		//		"mapreduce.input.lineinputformat.linespermap", 10000);
		//FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		//NLineInputFormat nlif=new NLineInputFormat();
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
	
		//job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		

		System.exit(job.waitForCompletion(true)?0:-1);
		
	}
}
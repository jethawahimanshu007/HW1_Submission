import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends
		Reducer<Text, LongWritable, Text, LongWritable> {

	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		
		
		int numberOfChapters=Integer.parseInt(context.getConfiguration().get(
		  "numberOfChapters"));
		  
		  int count=0; ArrayList<Integer> al=new ArrayList<Integer>();
		  while(values.iterator().hasNext()) { LongWritable
		  lw=values.iterator().next(); if(!al.contains((int)(lw.get()))) {
		  al.add((int)(lw.get())); } count+=1; }
		  
		  if(al.size()==numberOfChapters ) context.write(new Text(key), new
		  LongWritable(count));
		 
	}
}

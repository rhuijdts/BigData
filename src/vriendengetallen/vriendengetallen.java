package vriendengetallen;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class vriendengetallen extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		FileUtils.deleteDirectory(new File("output"));
		
		vriendengetallen vg = new vriendengetallen();
		int tx = ToolRunner.run(vg, args);
		System.exit(tx);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Job job = Job.getInstance();
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path("input"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		
		job.setMapperClass(vriendengetallenMapper.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		
		job.setReducerClass(vriendengetallenReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		Date start = new Date();
		
		if(job.waitForCompletion(true)){
			Date end = new Date();
			long elapsed = end.getTime() - start.getTime();
			Logger l = Logger.getLogger(vriendengetallen.class.getName());
			l.log(Level.INFO, "Totale tijd -> " + elapsed);
			return 0;
		} else {
			return 1;
		}
	
	}
}

class vriendengetallenMapper extends Mapper<LongWritable, Text, Text, Text>{
	int inputNumber;
	private int amicableProduct;
	private ArrayList<Integer> indentityMap = new ArrayList<>();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] line = value.toString().split(" ");
		for (String s : line) {
			inputNumber = Integer.parseInt(s);			
			
				amicableProduct = sumFactors(inputNumber);				
				
				if (sumFactors(amicableProduct) == inputNumber) {
					System.out.println(inputNumber + "");
					context.write(new Text(""), new Text(inputNumber + " " + amicableProduct + " "));
				}
			
		}
	}

	private int sumFactors(int inputNummer) {
		int som = 0;
		//int max_height = (int)Math.sqrt(inputNummer);	
		int incrementer = 1;
        if (inputNummer % 2 != 0)
        {
            incrementer = 2;
        }
        for (int i = 1; i <= inputNummer / 2; i=i+incrementer)
        {
            if (inputNummer % i == 0)
            {
                som += i;
            }
        }
        
		return som;
	}
}

class vriendengetallenReducer extends Reducer<Text, IntWritable, Text, Text>{
	private Scanner scanner;

	public void reduce(Text key, Text values, Context context) throws IOException, InterruptedException {

		HashMap<Integer, Integer> hash = new HashMap<>();
		
		scanner = new Scanner(values.toString());
		scanner.useDelimiter(" ");
		

		while (scanner.hasNext()) {
			hash.put(scanner.nextInt(), scanner.nextInt());
		}
		for (Integer sleutelwaarde : hash.keySet()) {
			int huidigeWaarde = hash.get(sleutelwaarde);
			for (Integer i : hash.keySet()) {
				System.out.println(i);
				if (huidigeWaarde == i && i != sleutelwaarde) {
					context.write(new Text("PAIR"), new Text("" + i + " - " + sleutelwaarde));
				}

			}
		}

	}

}

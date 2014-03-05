package it.polimi.hadoop;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MedianLoadJob extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		System.out.println("Start Run");

		Path pt = new Path(args[0]);
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(pt)));
		String line;
		line = br.readLine();
		System.out.println(line);
		Configuration jobConf = getConf();
		jobConf.set("time", line.split(",")[1]);

		@SuppressWarnings("deprecation")
		Job job = new Job(jobConf, "Median Load");

		job.setJarByClass(getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(MedianLoadMapper.class);
		job.setReducerClass(MedianLoadReducer.class);

		job.setMapOutputKeyClass(HomeHourWritable.class);
		job.setMapOutputValueClass(PlugMeasureWritable.class);

		job.setOutputKeyClass(HomeHourWritable.class);
		job.setOutputValueClass(FloatWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MedianLoadJob(), args);
		System.exit(exitCode);
	}

}

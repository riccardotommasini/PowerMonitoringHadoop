package it.polimi.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MedianLoadMapper extends
		Mapper<LongWritable, Text, HomeHourWritable, PlugMeasureWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] parts = line.split(",");
		if (parts.length < 6) {
			return;
		}

		Configuration conf = context.getConfiguration();
		String param = conf.get("time");

		Long timestamp = Long.parseLong(parts[1]);
		Long hour = (timestamp - Long.parseLong(param)) / 3600;
		Long measurement = Long.parseLong(parts[5]);
		int plugID = Integer.parseInt(parts[2]);
		int houseID = Integer.parseInt(parts[4]);
		context.write(new HomeHourWritable(houseID, hour),
				new PlugMeasureWritable(plugID, measurement));

	}

}

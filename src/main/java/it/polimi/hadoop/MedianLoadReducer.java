package it.polimi.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MedianLoadReducer
		extends
		Reducer<HomeHourWritable, PlugMeasureWritable, HomeHourWritable, FloatWritable> {

	@Override
	protected void reduce(HomeHourWritable key,
			Iterable<PlugMeasureWritable> values, Context context)
			throws IOException, InterruptedException {

		int medianGT = 0;

		List<Long> vals = new ArrayList<Long>();
		Map<Integer, List<Long>> plugMeasures = new HashMap<Integer, List<Long>>();

		for (PlugMeasureWritable pMW : values) {

			vals.add(pMW.getMeasure().get());

			Integer plugid = new Integer(pMW.getPlug().get());
			Long measure = pMW.getMeasure().get();

			if (plugMeasures.containsKey(plugid)) {
				plugMeasures.get(plugid).add(measure);
			} else {
				plugMeasures.put(plugid,
						new ArrayList<Long>(Arrays.asList(measure)));
			}

		}

		Long med = median(vals);

		for (Integer plug : plugMeasures.keySet()) {
			if (median(plugMeasures.get(plug)) > med)
				medianGT++;
		}

		medianGT *= 100;

		context.write(key, new FloatWritable(medianGT
				/ plugMeasures.keySet().size()));
	}

	private Long median(List<Long> vals) {
		int size = vals.size();
		Collections.sort(vals);
		if (size % 2 == 0) {
			return ((vals.get(size / 2 - 1) + (vals.get(size / 2)) / 2));
		} else
			return vals.get(size / 2);
	}

}

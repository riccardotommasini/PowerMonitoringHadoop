package it.polimi.pig;

import java.util.Collections;
import java.util.List;

public class Median {

	public Median() {
		// TODO Auto-generated constructor stub
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

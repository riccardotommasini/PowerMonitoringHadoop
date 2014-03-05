package it.polimi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class HomeHourWritable implements WritableComparable<HomeHourWritable> {

	private IntWritable house;
	private LongWritable hour;

	public HomeHourWritable(int house, long hour) {
		this.house = new IntWritable(house);
		this.hour = new LongWritable(hour);
	}

	public HomeHourWritable() {
		this.house = new IntWritable();
		this.hour = new LongWritable();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.house.write(out);
		this.hour.write(out);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.house.readFields(in);
		this.hour.readFields(in);
	}

	@Override
	public int compareTo(HomeHourWritable o) {

		if (o == null) {
			throw new NullPointerException();
		} else if (o == this) {
			return 0;
		}
		if (this.house.compareTo(o.house) == 0) {
			return this.hour.compareTo(o.hour);
		} else
			return this.house.compareTo(o.house);

	}

	public IntWritable getHouse() {
		return house;
	}

	public void setHouse(IntWritable house) {
		this.house = house;
	}

	public LongWritable getHour() {
		return hour;
	}

	public void setHour(LongWritable hour) {
		this.hour = hour;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((hour == null) ? 0 : hour.hashCode());
		result = prime * result + ((house == null) ? 0 : house.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		HomeHourWritable other = (HomeHourWritable) obj;
		if (hour == null) {
			if (other.hour != null)
				return false;
		} else if (!hour.equals(other.hour))
			return false;
		if (house == null) {
			if (other.house != null)
				return false;
		} else if (!house.equals(other.house))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "HomeHourKeyWritable [house=" + house + ", hour=" + hour + "]";
	}

}

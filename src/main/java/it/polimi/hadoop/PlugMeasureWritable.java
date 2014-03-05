package it.polimi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class PlugMeasureWritable implements
		WritableComparable<PlugMeasureWritable> {

	private IntWritable plug;

	private LongWritable measure;

	public PlugMeasureWritable(int plug, long measure) {
		this.plug = new IntWritable(plug);
		this.measure = new LongWritable(measure);
	}

	public PlugMeasureWritable() {
		this.plug = new IntWritable();
		this.measure = new LongWritable();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.plug.write(out);
		this.measure.write(out);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.plug.readFields(in);
		this.measure.readFields(in);
	}

	@Override
	public int compareTo(PlugMeasureWritable o) {

		if (o == null) {
			throw new NullPointerException();
		} else if (o == this) {
			return 0;
		}
		if (this.plug.compareTo(o.plug) == 0) {
			return this.measure.compareTo(o.measure);
		} else
			return this.plug.compareTo(o.plug);

	}

	public IntWritable getHouse() {
		return plug;
	}

	public void setHouse(IntWritable house) {
		this.plug = house;
	}

	public IntWritable getPlug() {
		return plug;
	}

	public void setPlug(IntWritable plug) {
		this.plug = plug;
	}

	public LongWritable getMeasure() {
		return measure;
	}

	public void setMeasure(LongWritable measure) {
		this.measure = measure;
	}

	@Override
	public String toString() {
		return "PlugMeasureWritable [plug=" + plug + ", measure=" + measure
				+ "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((measure == null) ? 0 : measure.hashCode());
		result = prime * result + ((plug == null) ? 0 : plug.hashCode());
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
		PlugMeasureWritable other = (PlugMeasureWritable) obj;
		if (measure == null) {
			if (other.measure != null)
				return false;
		} else if (!measure.equals(other.measure))
			return false;
		if (plug == null) {
			if (other.plug != null)
				return false;
		} else if (!plug.equals(other.plug))
			return false;
		return true;
	}

}

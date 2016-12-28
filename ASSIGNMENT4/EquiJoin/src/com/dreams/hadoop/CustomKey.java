package com.dreams.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * CustomKey
 * @author Selva
 *
 */
public class CustomKey implements WritableComparable<CustomKey> {
	
	private Integer storeId;
	private Integer dataSetType;

	public CustomKey() {
	}
	
	public CustomKey(Integer storeId, Integer dataSetType) {
		super();
		this.storeId = storeId;
		this.dataSetType = dataSetType;
	}
	
	public Integer getStoreId() {
		return storeId;
	}
	
	public Integer getDataSetType() {
		return dataSetType;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(storeId);
		out.writeInt(dataSetType);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		storeId = in.readInt();
		dataSetType = in.readInt();
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((dataSetType == null) ? 0 : dataSetType.hashCode());
		result = prime * result + ((storeId == null) ? 0 : storeId.hashCode());
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
		CustomKey other = (CustomKey) obj;
		if (dataSetType == null) {
			if (other.dataSetType != null)
				return false;
		} else if (!dataSetType.equals(other.dataSetType))
			return false;
		if (storeId == null) {
			if (other.storeId != null)
				return false;
		} else if (!storeId.equals(other.storeId))
			return false;
		return true;
	}
	
	@Override
	public int compareTo(CustomKey o) {
		int returnValue = compare(storeId, o.getStoreId());
		if (returnValue != 0) {
			return returnValue;
		}
		return compare(dataSetType, o.getDataSetType());
	}
	
	public static int compare(int k1, int k2) {
		return (k1 < k2 ? -1 : (k1 == k2 ? 0 : 1));
	}
	
	@Override
	public String toString() {
		return "CustomKey [storeId=" + storeId + ", dataSetType=" + dataSetType + "]";
	}

}

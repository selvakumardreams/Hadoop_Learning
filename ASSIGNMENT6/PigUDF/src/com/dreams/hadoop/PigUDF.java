package com.dreams.hadoop;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * 
 * @author m1013673
 *
 */
public class PigUDF extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0){
			return "";
		}

		int numOfInputs = input.size();
		StringBuilder concatValue = new StringBuilder("");

		for(int i = 0; i < numOfInputs; i++){
			Object value = (Object) input.get(i);
			if(value != null){
				concatValue.append(value);
			}
		}
		return concatValue.toString();

	}

}

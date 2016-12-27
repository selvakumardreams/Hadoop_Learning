package com.dreams.hadoop;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
 
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
/**
 * MapSideJoinMapper
 * @author Selva
 *
 */
@SuppressWarnings("deprecation")
public class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
 
	private static HashMap<String, String> storeOrderMap = new HashMap<String, String>();
	private BufferedReader brReader;
	private String dataValue = "";
	private Text outKey = new Text("");
	private Text outValue = new Text("");
 
	enum MYCOUNTER {
		RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
	}
 
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
 
		Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());
 
		for (Path eachPath : cacheFilesLocal) {
			if (eachPath.getName().toString().trim().equals("module5_ex1_f1")) {
				context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
				setupHashMap(eachPath, context);
			}
		}
 
	}
 
	/**
	 * setupHashMap
	 * @param filePath
	 * @param context
	 * @throws IOException
	 */
	private void setupHashMap(Path filePath, Context context)
			throws IOException {
 
		String strLineRead = "";
 
		try {
			System.out.println("MapSideJoin: " + filePath.toString());
			brReader = new BufferedReader(new FileReader(filePath.toString()));
 
			while ((strLineRead = brReader.readLine()) != null) {
				String keyOrderArr[] = strLineRead.toString().split("\\t+");
				System.out.println("MapSideJoin: keyOrderMap:  " + keyOrderArr[0].trim() + ":" + keyOrderArr[1].trim());
				storeOrderMap.put(keyOrderArr[0].trim(),	keyOrderArr[1].trim());
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			context.getCounter(MYCOUNTER.FILE_NOT_FOUND).increment(1);
		} catch (IOException e) {
			context.getCounter(MYCOUNTER.SOME_OTHER_ERROR).increment(1);
			e.printStackTrace();
		}finally {
			if (brReader != null) {
				brReader.close();
 
			}
 
		}
	}
 
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
 
		context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);
 
		if (value.toString().length() > 0) {
			String storeDataArr[] = value.toString().split("\\t+");
 
			try {
				dataValue = storeOrderMap.get(storeDataArr[0].toString());
			} finally {
				dataValue = ((dataValue.equals(null) || dataValue
						.equals("")) ? "NOT-FOUND" : dataValue);
			}
 
			outKey.set(storeDataArr[0].toString());
 
			outValue.set(storeDataArr[1].toString() + "\t" +  dataValue);
 
		}
		context.write(outKey, outValue);
		dataValue = "";
	}
}

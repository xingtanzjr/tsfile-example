package thu.tsfile.example;
/**
 * There are two ways to construct a TsFile instance,they generate the same TsFile file.
 * The class use the second interface: 
 *     public void addMeasurement(MeasurementDescriptor measurementDescriptor) throws WriteProcessException
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.FloatDataPoint;

public class TsFileWriteDemo {

	public static final String INPUT_DATA_FILE_PATH = "src/main/resources/demoData.csv";
	public static final String OUTPUT_TSFILE_PATH = "src/main/resources/demo.ts";
	private static final String PATH_SPILTER = "\\.";
	private static final String CSV_SPILTER = "\\,";

	private static List<String> measurements;
	private static String deviceName;

	public static void main(String args[]) throws IOException, WriteProcessException {
		BufferedReader reader = new BufferedReader(new FileReader(INPUT_DATA_FILE_PATH));
		String header = reader.readLine();
		System.out.println("Prepare header accroding to inputFile.");
		prepareSchema(header);
		TsFileWriter writer = new TsFileWriter(new File(OUTPUT_TSFILE_PATH));
		System.out.println("create metadata for current TsFile.");
		createMetadata(writer);
		System.out.println("start to write TsFile.");
		String line;
		while ((line = reader.readLine()) != null) {
			writer.write(genRecordFromString(line));
		}
		writer.close();
		reader.close();
		System.out.println("Write TsFile Done. FileName is :" + OUTPUT_TSFILE_PATH);
	}

	private static void prepareSchema(String metadataInString) {
		measurements = new ArrayList<>();
		String[] keywords = metadataInString.split(CSV_SPILTER);
		for (int i = 1; i < keywords.length; i++) {
			setDeviceName(keywords[i]);
			measurements.add(getMeasurementByFullName(keywords[i]));
		}
	}

	private static long getTimestamp(String timeInString) {
		DateTime dateTime = new DateTime(timeInString);
		return dateTime.getMillis();
	}

	private static String getMeasurementByFullName(String seriesFullName) {
		String[] keywords = seriesFullName.split(PATH_SPILTER);
		return keywords[keywords.length - 1];
	}

	private static void setDeviceName(String seriesFullName) {
		String curDeviceName = seriesFullName.substring(0, seriesFullName.lastIndexOf("."));
		if (deviceName == null) {
			deviceName = curDeviceName;
		} else {
			if (!deviceName.equals(curDeviceName)) {
				throw new RuntimeException("Not consistent device name");
			}
		}
	}

	private static TSRecord genRecordFromString(String line) {
		String[] keywords = line.split(CSV_SPILTER);
		TSRecord record = new TSRecord(getTimestamp(keywords[0]), deviceName);
		for (int i = 1; i < keywords.length; i++) {
			record.addTuple(new FloatDataPoint(measurements.get(i - 1), Float.valueOf(keywords[i])));
		}
		return record;
	}

	private static void createMetadata(TsFileWriter writer) throws WriteProcessException {
		writer.addMeasurement(new MeasurementDescriptor("axis1pos", TSDataType.FLOAT, TSEncoding.RLE));
		writer.addMeasurement(new MeasurementDescriptor("axis2pos", TSDataType.FLOAT, TSEncoding.RLE));
		writer.addMeasurement(new MeasurementDescriptor("axis3pos", TSDataType.FLOAT, TSEncoding.RLE));
		writer.addMeasurement(new MeasurementDescriptor("axis4pos", TSDataType.FLOAT, TSEncoding.RLE));
		writer.addMeasurement(new MeasurementDescriptor("axis1vel", TSDataType.FLOAT, TSEncoding.RLE));
		writer.addMeasurement(new MeasurementDescriptor("axis2vel", TSDataType.FLOAT, TSEncoding.RLE));
		writer.addMeasurement(new MeasurementDescriptor("axis3vel", TSDataType.FLOAT, TSEncoding.RLE));
		writer.addMeasurement(new MeasurementDescriptor("axis4vel", TSDataType.FLOAT, TSEncoding.RLE));
		writer.addMeasurement(new MeasurementDescriptor("axis1torque", TSDataType.FLOAT, TSEncoding.RLE));
		writer.addMeasurement(new MeasurementDescriptor("axis2torque", TSDataType.FLOAT, TSEncoding.RLE));
		writer.addMeasurement(new MeasurementDescriptor("axis3torque", TSDataType.FLOAT, TSEncoding.RLE));
		writer.addMeasurement(new MeasurementDescriptor("axis4torque", TSDataType.FLOAT, TSEncoding.RLE));
		writer.addMeasurement(new MeasurementDescriptor("axis1set", TSDataType.FLOAT, TSEncoding.RLE));
		writer.addMeasurement(new MeasurementDescriptor("axis2set", TSDataType.FLOAT, TSEncoding.RLE));
		writer.addMeasurement(new MeasurementDescriptor("axis3set", TSDataType.FLOAT, TSEncoding.RLE));
		writer.addMeasurement(new MeasurementDescriptor("axis4set", TSDataType.FLOAT, TSEncoding.RLE));
		writer.addMeasurement(new MeasurementDescriptor("v_x", TSDataType.FLOAT, TSEncoding.RLE));
		writer.addMeasurement(new MeasurementDescriptor("v_y", TSDataType.FLOAT, TSEncoding.RLE));
		writer.addMeasurement(new MeasurementDescriptor("v_angle", TSDataType.FLOAT, TSEncoding.RLE));
		writer.addMeasurement(new MeasurementDescriptor("moving", TSDataType.FLOAT, TSEncoding.RLE));
		writer.addMeasurement(new MeasurementDescriptor("cpu0temp", TSDataType.FLOAT, TSEncoding.RLE));
	}
}
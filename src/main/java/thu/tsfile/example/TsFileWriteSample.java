package thu.tsfile.example;

import java.io.File;
import java.io.IOException;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.FloatDataPoint;

public class TsFileWriteSample {

	public static final String OUTPUT_TSFILE_PATH = "src/main/resources/demo.ts";

	public static void main(String args[]) throws WriteProcessException, IOException {
		TsFileWriter writer = new TsFileWriter(new File(OUTPUT_TSFILE_PATH));
		writer.addMeasurement(new MeasurementDescriptor("axis1pos", TSDataType.FLOAT, TSEncoding.RLE));
		
		TSRecord tsRecord1 = new TSRecord(1502089355159L, "root.yanmoji.shenzhen.d1");
		tsRecord1.addTuple(new FloatDataPoint("axis1pos", -61.9f));
		TSRecord tsRecord2 = new TSRecord(1502089355160L, "root.yanmoji.shenzhen.d1");
		tsRecord2.addTuple(new FloatDataPoint("axis1pos", 51.9f));

		writer.write(tsRecord1);
		writer.write(tsRecord2);
		
		writer.close();
	}
}
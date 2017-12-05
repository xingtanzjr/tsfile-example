package thu.tsfile.example;

import java.io.IOException;
import java.util.ArrayList;

import cn.edu.tsinghua.tsfile.timeseries.basis.TsFile;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

public class TsFileReadDemo {

	public static void main(String args[]) throws IOException {
		String inputFilePath = TsFileWriteDemo.OUTPUT_TSFILE_PATH;

		TsRandomAccessLocalFileReader inputFile = new TsRandomAccessLocalFileReader(inputFilePath);
		TsFile tsFile = new TsFile(inputFile);

		ArrayList<Path> paths = new ArrayList<>();
		//  time >= 1502089355159 and time < 1502089357560
		FilterExpression timeFilter = FilterFactory.and(
				FilterFactory.gtEq(FilterFactory.timeFilterSeries(), 1502089355159L, true),
				FilterFactory.ltEq(FilterFactory.timeFilterSeries(), 1502089357560L, false));
		// axis1pos < 30
		FilterExpression valueFilter = FilterFactory
				.ltEq(FilterFactory.floatFilterSeries("root.yanmoji.shenzhen.d1", "axis1pos", FilterSeriesType.VALUE_FILTER), 30f, false);
		
		paths.add(new Path("root.yanmoji.shenzhen.d1.axis1pos"));
		long startTimestamp = System.currentTimeMillis();
		QueryDataSet queryDataSet = tsFile.query(paths, timeFilter, valueFilter);
		System.out.println("\nstart execute query.\n--------------");
		while (queryDataSet.hasNextRecord()) {
			System.out.println(queryDataSet.getNextRecord());
		}
		long endTimestamp = System.currentTimeMillis();
		System.out.println("------------ Time: " + (endTimestamp - startTimestamp) + "ms");
	}
}

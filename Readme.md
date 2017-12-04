## TsFile-Example

This project is mainly used to introduce the basic usages of TsFile and IoTDB-JDBC.

### TsFile-Demo

For TsFile, this project supplies two simple classes to show how to write a TsFile and how to execute query on specified TsFile. These two classes are ```TsFileWriteDemo``` and ```TsFileReadDemo```.

#### Write a TsFile

Run ```TsFileWriteDemo``` to write a TsFile. In this class, the constant ```INPUT_DATA_FILE_PATH``` represents the path of data-source file and the constant ```OUTPUT_TSFILE_PATH``` represents the output path of generated TsFile. The default folder for these two files is ```src/main/resources``` and you can modify these two constants according to your own situation.

#### Read from TsFile

To construct specific query, you can modify the variables ```timeFilter``` and ```valueFilter``` in class ```TsFileReadDemo ```.

### IoTDB-JDBC-Demo

Just Run ```JDBCDemo``` to execute query from JDBC. You can modify the constant ```QUERY_SQL``` to execute specific statements.
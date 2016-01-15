package com.infobird.parquet.AvroParquet;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.fs.Path;

import parquet.avro.AvroSchemaConverter;
import parquet.avro.AvroWriteSupport;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

import com.infobird.data.entity.User;

public class AvroParquetFile {

	private static String inputPath = "D:/workspaces/others/AvroDemo/users1.avro";
	private static String schemaPath="D:/workspaces/others/AvroDemo/src/main/avro/users.avsc";
	
	private static String outFileName="users_1.parquet";
	
	
	public static void main(String[] args) {
		
		if(args.length == 3) {
			inputPath = args[0];
			schemaPath = args[1];
			outFileName = args[2];
		}
		
		File file = new File(schemaPath);
		File inputFile = new File(inputPath);
		InputStream in;
		try {
			
			in = new FileInputStream(file);
			Schema avroSchema = new Schema.Parser().parse(in);
			System.out.println(new AvroSchemaConverter().convert(avroSchema).toString());
			
			// generate the corresponding Parquet schema
			MessageType parquetSchema = new AvroSchemaConverter().convert(avroSchema);
			 
			// create a WriteSupport object to serialize your Avro objects
			WriteSupport<IndexedRecord> writeSupport = new AvroWriteSupport(parquetSchema, avroSchema);
			 
			// choose compression scheme
			CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;
			 
			// set Parquet file block size and page size values
			int blockSize = 256 * 1024 * 1024;
			int pageSize = 64 * 1024;
			 
			Path outputPath = new Path(outFileName);
				
		/*	ParquetWriter parquetWriter = new AvroParquetWriter(outputPath,
			          avroSchema, compressionCodecName, blockSize, pageSize);*/
			
		
			
			// the ParquetWriter object that will consume Avro GenericRecords
			ParquetWriter<IndexedRecord> parquetWriter = new ParquetWriter<IndexedRecord>(outputPath,
			        writeSupport, compressionCodecName, blockSize, pageSize);
			

			
			DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
			DataFileReader<User> dataFileReader = null;

			try {
				User user = null;
				dataFileReader = new DataFileReader<User>(inputFile, userDatumReader);
				while(dataFileReader.hasNext()) {
					user = dataFileReader.next(user);
					System.out.println("user:" + user);
				    parquetWriter.write(user);
				    parquetWriter.close();
				}
				
				 
				
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	    

	   
	}
}

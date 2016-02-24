package main;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.tika.Tika;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class SimpleTextExtractor {
	static final String rootDIR = "/home/roohy/PycharmProjects/CS599_HW1/S3_Downloader";
	static final String dumpDIR = "/home/roohy/PycharmProjects/CS599_HW1/dumped";
	static final String mongoURL = "mongodb://admin:admin123@localhost:27017";
	static final String databaseName = "CS599_HW1";
	static final int maxSize = 50000;
	
	public static void main(String[] args) throws Exception{
	
		Tika tika = new Tika();
		
		MongoClientURI connectionString = new MongoClientURI(mongoURL);
		MongoClient mongoClient = new MongoClient(connectionString);
		MongoDatabase database = mongoClient.getDatabase(databaseName);
		//String text = tika.parseToString(new File("/home/roohy/PycharmProjects/CS599_HW1/S3_Downloader/55/62/116/129/029CE528D559EE50DAD5F5BE500FF7051DFE85C01B6370388B94208A3F07F082"));
		//System.out.print(text);
		/*for (String file : args){
			String text = tika.parseToString(new File(file));
			System.out.print(text);
		}*/
		//database.createCollection("files");
		MongoCollection<Document> collection = database.getCollection("test");
		
		ArrayList<File> files = new ArrayList<File>();
		listf(rootDIR, files);
		List<Document> documents = new ArrayList<Document>();
		for (File file: files){
			//String type = tika.detect(file);
			documents.add(new Document("path",file.getAbsolutePath())
			.append("type", tika.detect(file)));
			
			//System.out.println(file.getAbsolutePath());//getPath());//type);
		}
		collection.insertMany(documents);
		System.out.print("number of documents is %d");
		System.out.print(collection.count());
		mongoClient.close();
	}
	
	public static void listf(String directoryName, ArrayList<File> files) {
	    File directory = new File(directoryName);

	    // get all the files from a directory
	    File[] fList = directory.listFiles();
	    for (File file : fList) {
	        if (file.isFile()) {
	            files.add(file);
	        } else if (file.isDirectory()) {
	            listf(file.getAbsolutePath(), files);
	        }
	    }
	}
}

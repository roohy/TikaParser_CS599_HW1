package main;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.tika.Tika;
import org.bson.Document;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class SimpleTextExtractor {
	static final String rootDIR = "/home/roohy/PycharmProjects/CS599_HW1/S3_Downloader";
	static final String dumpDIR = "/home/roohy/PycharmProjects/CS599_HW1/dumped";
	static final String mongoURL = "mongodb://admin:admin123@localhost:27017";
	static final String databaseName = "CS599_HW1";
	static final String POLAR = "polar-fulldump";
	static final int maxSize = 50000;
	
	public static void main(String[] args) throws Exception{
	
		Tika tika = new Tika();
		
		MongoClientURI connectionString = new MongoClientURI(mongoURL);
		MongoClient mongoClient = new MongoClient(connectionString);
		MongoDatabase database = mongoClient.getDatabase(databaseName);
		//database.createCollection("files");
		MongoCollection<Document> collection = database.getCollection("files");
		
		int totalCount = 0;
		AmazonS3 s3 = new AmazonS3Client();
		//Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        //s3.setRegion(usWest2);
        /*
        System.out.println("Listing buckets");
        for (Bucket bucket : s3.listBuckets()) {
            System.out.println(" - " + bucket.getName());
        }*/
        
		
        System.out.println("Listing objects");
        ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
                .withBucketName(POLAR));
        S3Object object = null;
        Boolean flag = true;
        FindIterable<Document> cursor = null;
        while(true){
        	for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                //System.out.println(" - " + objectSummary.getKey() + "  " +
                  //      "(size = " + objectSummary.getSize() + ")");
        		totalCount++;
        		
        		
                BasicDBObject query = new BasicDBObject("key",objectSummary.getKey());
                cursor = collection.find(query);
                if(cursor.iterator().hasNext()){
                	//System.out.println("duplicate");
                	if(flag)
                	{
                		flag = false;
                		System.out.println("***********repeating ");
                	}
                	continue;
                }
                else{
                	if(!flag)
                	{
                		flag = true;
                		System.out.println("not repeating "+totalCount);
                	}
                }
                try{
                    object = s3.getObject(new GetObjectRequest(POLAR, objectSummary.getKey()));
            		}
            		catch(Exception e){
            			System.out.println("total count "+totalCount);
            			return;
            		}
                Document doc = new Document("key",objectSummary.getKey());
                S3ObjectInputStream inputStream = object.getObjectContent();
                doc.append("type", tika.detect(inputStream));

//                System.out.println("Content-Type: "  + doc.toJson());
                collection.insertOne(doc);
                inputStream.close();
                
            }
        	
        	if(objectListing.isTruncated()){
        		System.out.println("Going for the next batch.");
        		objectListing = s3.listNextBatchOfObjects(objectListing);
        	}
        	else{
        		System.out.println("Last batch ended");
        		break;
        	}
        }

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
/*


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
		}
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


*/
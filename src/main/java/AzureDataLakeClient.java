import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.stream.Collectors;


import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;


public class AzureDataLakeClient {

	private static final String accountName ="datalaketestintelie";
	private static final String accountKey =
	"nuBaXNp4H9GyGsSHd0DrO8pBPwp4o+7X3ThtLa0jp4G02m4diQ2QWvgsbEyxSeOYh7dRcbnRjrJiEnxT1aKviQ==";
	private static final String fileSystemName = "datainput";
	
	
	public static void main(String[] args) throws FileNotFoundException, IOException {
		DataLakeServiceClient cli = GetDataLakeServiceClient(accountName,accountKey);
		DataLakeFileSystemClient fileSystemClient = cli.getFileSystemClient(fileSystemName);
		DownloadFile4(fileSystemClient);
	}
	
	
	static public DataLakeServiceClient GetDataLakeServiceClient
	(String accountName, String accountKey){

	    StorageSharedKeyCredential sharedKeyCredential =
	        new StorageSharedKeyCredential(accountName, accountKey);

	    DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder();
	    builder.credential(sharedKeyCredential);
	    builder.endpoint("https://" + accountName + ".dfs.core.windows.net");

	    return builder.buildClient();
	}
	
	static public void DownloadFile(DataLakeFileSystemClient fileSystemClient)
		    throws FileNotFoundException, java.io.IOException{

		    DataLakeFileClient fileClient = 
		    		fileSystemClient.getFileClient("teste.txt");
			    ByteArrayOutputStream os = new ByteArrayOutputStream();
			    fileClient.read(os);
			    String out = new String(os.toByteArray(), "UTF-8");
			    System.out.println(out);
			    os.close();
		      
	}
	
	static public void DownloadFile2(DataLakeFileSystemClient fileSystemClient)
		    throws FileNotFoundException, java.io.IOException{

		    DataLakeFileClient fileClient = 
		    		fileSystemClient.getFileClient("teste.txt");
		    String expression = "SELECT * from BlobStorage";
		    InputStream is = fileClient.openQueryInputStream(expression);
		    String result = new BufferedReader(new InputStreamReader(is))
		    		  .lines().collect(Collectors.joining("\n"));
		    System.out.println(result);
	}

	static public void DownloadFile3(DataLakeFileSystemClient fileSystemClient)
			throws FileNotFoundException, java.io.IOException{

		DataLakeFileClient fileClient =
				fileSystemClient.getFileClient("teste.txt.gz");
		TestOutputStream os = new TestOutputStream();
		fileClient.read(os);
		InputStream gzis = os.toGZIPInputStream();
		File file = new File("/home/andres/development/code/read-datalake/teste.txt");

		FileOutputStream fos = new FileOutputStream(file);
		byte[] buffer = new byte[1024];
		int len = 0;
		while ((len = gzis.read(buffer)) > 0) {
			fos.write(buffer, 0, len);
		}

		fos.close();
		gzis.close();

	}
	
	static public void DownloadFile4(DataLakeFileSystemClient fileSystemClient)
			throws FileNotFoundException, java.io.IOException{

		DataLakeFileClient fileClient =
				fileSystemClient.getFileClient("teste.txt.gz");
		TestOutputStream os = new TestOutputStream();
		fileClient.read(os);
		
		InputStream gzis = os.toGZIPInputStream();
		FileChannel fileChannel = FileChannel.open(
				Paths.get("/Users/andresromero/eclipse-workspace-2020/read-datalake/teste.txt"), 
				StandardOpenOption.CREATE,StandardOpenOption.WRITE);
		
		
		byte[] buf = new byte[1024];
		ByteBuffer buffer = ByteBuffer.wrap(buf);
		while (gzis.read(buf) > 0) {
			fileChannel.write(buffer);

		}
		fileChannel.close();
		gzis.close();

	}
	

}

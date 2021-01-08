package B2.HW;

import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.backblaze.b2.client.B2ListFilesIterable;
import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.contentHandlers.B2ContentMemoryWriter;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.structures.B2Bucket;
import com.backblaze.b2.client.structures.B2FileVersion;
import com.backblaze.b2.client.structures.B2ListBucketsResponse;
import com.backblaze.b2.client.webApiHttpClient.B2StorageHttpClientBuilder;

/**
 * Count number of characters in all the files in all the buckets for an account
 *
 */
public class CharCount {
//    private static Pattern aWord = Pattern.compile("\\ba"); // regular expression to find word a 
    private static Pattern aLetter = Pattern.compile("a"); // regular expression to find letter a
    private static final String USER_AGENT = "B2HomeWork";

    public static void main( String[] args ) {
    	if(args.length != 2) {
			System.err.println("Please provide application_id and application_key");
			System.exit(1);    		
    	}
    	
	    final SortedSet<String> result = new TreeSet<String>(new Comparator<String>() {
	    	/* Custom comparator:
	    	 * Each parameter contains character count followed by file name. 
	    	 * Primary sort is by character count and secondary sort is by file name.
	    	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object) for java doc
	    	 */
	    	public int compare(String str1, String str2) {
				String[] string1 = str1.split(" ");
				String[] string2 = str2.split(" ");
				int count1 = Integer.parseInt(string1[0]);
				int count2 = Integer.parseInt(string2[0]);
				return (count1 != count2)? count1 - count2: string1[1].compareTo(string2[1]);
		}});

		String appId = args[0];
		String appKey = args[1];
		final B2StorageClient client = B2StorageHttpClientBuilder.builder(appId, appKey, USER_AGENT).build();
		try {
			B2ListBucketsResponse resp = client.listBuckets();
			List<B2Bucket> buckets = resp.getBuckets();
			for(final B2Bucket bucket : buckets) {				
				B2ListFilesIterable files = client.fileNames(bucket.getBucketId());

				//count occurrences of letter "a" in each file
				files.forEach(new Consumer<B2FileVersion>() {
					public void accept(B2FileVersion file) {
						B2ContentMemoryWriter sink = B2ContentMemoryWriter.build();
						try {
							client.downloadById(file.getFileId(), sink);
						} catch (B2Exception e) {
							//exit if there is an error downloading a file
							System.err.println(e.getMessage());
							System.exit(e.getStatus());
						}
						
						String fileContents = new String(sink.getBytes());
						Matcher matcher = aLetter.matcher(fileContents);
						int count = 0;
						while(matcher.find()) {
							count++;
						}
						result.add(count + " " + file.getFileName());
					}
				});
			}
			//print the result
			for(String resultLine : result) {
				System.out.println(resultLine);
			}
		} catch (B2Exception e) {
			System.err.println(e.getMessage());
			System.exit(e.getStatus());
		}
    }
}

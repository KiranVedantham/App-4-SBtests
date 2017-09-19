package com.swisscom.api.controllers;

import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.log;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import com.swisscom.api.AllServiceConfiguration;
import com.swisscom.api.ResourceUtil;

@RestController
@RequestMapping("/dynastg")
public class DynaStgController {

	@Autowired
	private AllServiceConfiguration sc;
	ResourceUtil rutil = new ResourceUtil();
	private static String bucketname = "testbucket-" + System.currentTimeMillis();

	@RequestMapping(value = "/createbucket", method = RequestMethod.GET)
	public HttpStatus createBuckt() throws Exception {
		createBucket();
		System.out.println("Bucket created");
		return HttpStatus.OK;
	}

	@RequestMapping(value = "/uploadfile", method = RequestMethod.GET)
	public HttpStatus upload() throws FileNotFoundException, Exception {

		upload(rutil.getFile("Sample.csv").getPath(), rutil.getFile("Sample.csv").getName());
		return HttpStatus.OK;
	}

	@RequestMapping(value = "/loadfiles", method = RequestMethod.GET)
	public HttpStatus uploadfile() throws FileNotFoundException, Exception {
		uploadfiles();
		return HttpStatus.OK;
	}

	@RequestMapping(value = "/loadfiles1/{size}", method = RequestMethod.GET)
	public HttpStatus uploadfile(@PathVariable String size) throws FileNotFoundException, Exception {
		int mb = Integer.parseInt(size);
		for (int i = 0; i < mb; i++) {
			uploadfiles();
		}
		return HttpStatus.OK;
	}

	// @RequestMapping(value = "/download", method = RequestMethod.GET)
	// public HttpStatus download() throws Exception {
	// download("Sample.csv");
	// return HttpStatus.OK;
	// }

	@RequestMapping(value = "/fetch", method = RequestMethod.GET)
	public HttpStatus lists() throws Exception {
		list();
		return HttpStatus.OK;
	}

	@RequestMapping(value = "/delete", method = RequestMethod.GET)
	public HttpStatus deletFile() throws Exception {
		deleteFile();
		return HttpStatus.OK;
	}

	@RequestMapping(value = "/deletebucket", method = RequestMethod.DELETE)
	public HttpStatus deletbucket() throws Exception {
		deleteBucket();
		return HttpStatus.OK;
	}

	public PutObjectResult upload(String filePath, String uploadKey) throws Exception {
		return upload(new FileInputStream(filePath), uploadKey);
	}

	private PutObjectResult upload(InputStream inputStream, String uploadKey) throws Exception {
		AmazonS3Client amazonS3Client = (AmazonS3Client) sc.getServiceInstance();
		PutObjectRequest putObjectRequest = new PutObjectRequest(bucketname, uploadKey, inputStream,
				new ObjectMetadata());

		putObjectRequest.setCannedAcl(CannedAccessControlList.PublicRead);

		PutObjectResult putObjectResult = amazonS3Client.putObject(putObjectRequest);

		IOUtils.closeQuietly(inputStream);

		return putObjectResult;
	}

	public List<PutObjectResult> upload(MultipartFile[] multipartFiles) {
		List<PutObjectResult> putObjectResults = new ArrayList<>();

		Arrays.stream(multipartFiles).filter(multipartFile -> !StringUtils.isEmpty(multipartFile.getOriginalFilename()))
				.forEach(multipartFile -> {
					try {
						putObjectResults
								.add(upload(multipartFile.getInputStream(), multipartFile.getOriginalFilename()));
					} catch (IOException e) {
						e.printStackTrace();
					} catch (Exception e) {

						e.printStackTrace();
					}
				});

		return putObjectResults;
	}

	public ResponseEntity<byte[]> download(String key) throws Exception {
		// GetObjectRequest getObjectRequest = new GetObjectRequest(bucket,
		// key);
		AmazonS3Client amazonS3Client = (AmazonS3Client) sc.getServiceInstance();
		S3Object s3Object = amazonS3Client.getObject(bucketname, key);

		S3ObjectInputStream objectInputStream = s3Object.getObjectContent();

		byte[] bytes = IOUtils.toByteArray(objectInputStream);

		String fileName = URLEncoder.encode(key, "UTF-8").replaceAll("\\+", "%20");

		HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.setContentType(MediaType.APPLICATION_OCTET_STREAM);
		httpHeaders.setContentLength(bytes.length);
		httpHeaders.setContentDispositionFormData("attachment", fileName);

		return new ResponseEntity<>(bytes, httpHeaders, HttpStatus.OK);
	}

	public List<S3ObjectSummary> list() throws Exception {
		AmazonS3Client amazonS3Client = (AmazonS3Client) sc.getServiceInstance();
		ObjectListing objectListing = amazonS3Client.listObjects(new ListObjectsRequest().withBucketName(bucketname));

		List<S3ObjectSummary> s3ObjectSummaries = objectListing.getObjectSummaries();
		System.out.println("summary " + s3ObjectSummaries.toString());
		return s3ObjectSummaries;
	}

	public void createBucket() throws Exception {
		AmazonS3Client amazonS3Client = (AmazonS3Client) sc.getServiceInstance();
		amazonS3Client.createBucket(bucketname);
	}

	public void deleteBucket() throws Exception {
		AmazonS3Client amazonS3Client = (AmazonS3Client) sc.getServiceInstance();

		try {
			System.out.println(" - removing objects from bucket");
			ObjectListing object_listing = amazonS3Client.listObjects(bucketname);
			while (true) {
				for (Iterator<?> iterator = object_listing.getObjectSummaries().iterator(); iterator.hasNext();) {
					S3ObjectSummary summary = (S3ObjectSummary) iterator.next();
					amazonS3Client.deleteObject(bucketname, summary.getKey());
				}

				// more object_listing to retrieve?
				if (object_listing.isTruncated()) {
					object_listing = amazonS3Client.listNextBatchOfObjects(object_listing);
				} else {
					break;
				}
			}
			System.out.println(" - removing versions from bucket");
			VersionListing version_listing = amazonS3Client
					.listVersions(new ListVersionsRequest().withBucketName(bucketname));
			while (true) {
				for (Iterator<?> iterator = version_listing.getVersionSummaries().iterator(); iterator.hasNext();) {
					S3VersionSummary vs = (S3VersionSummary) iterator.next();
					amazonS3Client.deleteVersion(

							bucketname, vs.getKey(), vs.getVersionId());
				}

				if (version_listing.isTruncated()) {
					version_listing = amazonS3Client.listNextBatchOfVersions(version_listing);
				} else {
					break;
				}
			}

			System.out.println(" OK, bucket ready to delete!");
			amazonS3Client.deleteBucket(bucketname);
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		}

	}

	public void deleteFile() throws AmazonServiceException, AmazonClientException, Exception {
		AmazonS3Client amazonS3Client = (AmazonS3Client) sc.getServiceInstance();
		amazonS3Client.deleteObject(bucketname, "Sample.csv");
	}

	public PutObjectResult uploadfiles() throws Exception {
		PutObjectResult result = new PutObjectResult();
		File folder = new File(rutil.getFile("upfiles").getPath());
		File[] listOfFiles = folder.listFiles();

		for (File file : listOfFiles) {
			if (file.isFile()) {
				result = upload(new FileInputStream(file.getPath()), file.getName());
				System.out.println("Uploaded file" + file.getName());
			}
		}
		return result;
	}
}
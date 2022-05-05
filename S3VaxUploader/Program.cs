using System.Net;

namespace S3VaxUploader;
using System;
using System.IO;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;


public class S3VaxUploader
{
    static void Main(string[] args)
    {
        // Check if the input arguments array contains the proper number of arguments
        if (args.Length != 2)
        {
            throw new ArgumentException("Required arguments: file-path data-type");
        }

        // Capture path from args array
        string path = @args[0];

        // Use FileInfo object to verify if the filepath points to an existing file.
        FileInfo file = new FileInfo(path);
        if (!file.Exists)
        {
            // Throw new exception if the file does not exist.
            throw new FileNotFoundException("File does not exist.");
        }

        // Capture file type from args array
        string type = args[1];
        
        // Check if it matches the two accepted filetypes.
        if (!String.Equals(type, "xml") && !String.Equals(type, "json"))
        {
            // Throw an exception if it does not match.
            throw new ArgumentException("Type must be equal to 'xml' or 'json'");
            // Console.WriteLine("Type must be equal to 'xml' or 'json'");
        }

        // If the arguments pass checks, use UploadFile method
        UploadFile(path, "vaccine-bucket", "test-file", type).Wait();
        Console.WriteLine("File uploading completed.");
    }

    /// <summary>
    /// Get AWS Credentials by profile name.
    /// </summary>
    /// <param name="profileName">The name of the profile.</param>
    /// <returns>The AWS Credentials</returns>
    /// <exception cref="ArgumentNullException">Profile cannot be null or empty.</exception>
    /// <exception cref="Exception">Profile not found.</exception>
    private static AWSCredentials GetAwsCredentials(string profileName)
    {
        if (String.IsNullOrEmpty(profileName))
        {
            throw new ArgumentNullException("profileName cannot be null or empty.");
        }

        SharedCredentialsFile credFile = new SharedCredentialsFile();
        CredentialProfile profile = credFile.ListProfiles().Find(p => p.Name.Equals(profileName));
        if (profile == null)
        {
            throw new Exception(String.Format("Profile named {0} not found.", profileName));
        }

        return AWSCredentialsFactory.GetAWSCredentials(profile, new SharedCredentialsFile());
    }

    private static async Task UploadFile(string filePath, string bucketName, string keyName, string type)
    {
        
        // If the arguments pass checks, create credentials file and S3Client objects
        AWSCredentials credentials = GetAwsCredentials("default");

        AmazonS3Client s3Client = new AmazonS3Client(credentials, RegionEndpoint.USEast1);

        try
        {
            PutObjectRequest putRequest = new PutObjectRequest
            {
                BucketName = bucketName,
                //Key = keyName,
                FilePath = filePath,
                ContentType = "text/" + type
            };

            PutObjectResponse response = await s3Client.PutObjectAsync(putRequest);
            Console.WriteLine("File Uploaded.");

            s3Client.Dispose();
            // return Task.CompletedTask;
        }
        catch (AmazonS3Exception e)
        {
            if (e.ErrorCode != null &&
                (e.ErrorCode.Equals("InvalidAccessKeyId")
                 ||
                 e.ErrorCode.Equals("InvalidSecurity")))
            {
                throw new Exception("Check the provided AWS Credentials.");
            }
            else
            {
                throw new Exception("Error occurred: " + e.Message);
            }
        }
    }
}
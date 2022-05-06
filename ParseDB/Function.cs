using System.Data;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Util;
using System.Xml;
using System.IO;
using System.Text;
using Npgsql;
using System.Data;
using System.Text.Json;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace ParseDB;

public class Function
{
    IAmazonS3 S3Client { get; set; }

    /// <summary>
    /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
    /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
    /// region the Lambda function is executed in.
    /// </summary>
    public Function()
    {
        S3Client = new AmazonS3Client();
    }

    /// <summary>
    /// Constructs an instance with a preconfigured S3 client. This can be used for testing the outside of the Lambda environment.
    /// </summary>
    /// <param name="s3Client"></param>
    public Function(IAmazonS3 s3Client)
    {
        this.S3Client = s3Client;
    }
    
    /// <summary>
    /// This method is called for every Lambda invocation. This method takes in an S3 event object and can be used 
    /// to respond to S3 notifications.
    /// </summary>
    /// <param name="evnt"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public async Task<string?> FunctionHandler(S3Event evnt, ILambdaContext context)
    {
        var s3Event = evnt.Records?[0].S3;
        if(s3Event == null)
        {
            return null;
        }

        string bucketName = s3Event.Bucket.Name;
        string objectKey = s3Event.Object.Key;
        Type objectType = s3Event.Object.GetType();
        string strType = objectType.ToString();

        // TODO
        // Get the object itself from S3 so we can parse it, we did this in Module 10

        // If the type of the object is json or xml
        if (strType == @"text/json")
        {
            // TODO 
            // GET FILE using the process outlined in Module 10
            try
            {
                // Create stream to get object from S3
                Stream stream = await S3Client.GetObjectStreamAsync(bucketName, objectKey, null);

                // Create content which will contain the contents of the file itself
                string content;

                // 
            }
            catch (Exception e)
            {
                context.Logger.LogInformation($"Error getting object {s3Event.Object.Key} from bucket {s3Event.Bucket.Name}. Make sure they exist and your bucket is in the same region as this function.");
                context.Logger.LogInformation(e.Message);
                context.Logger.LogInformation(e.StackTrace);
                throw;
            }

            // Parse JSON
            // JsonParseUploader(doc);

        } else if (strType == @"text/xml")
        {
            // TODO
            // GET FILE using the process outlined in Module 10
            try
            {
                // Create stream to get object from S3
                Stream stream = await S3Client.GetObjectStreamAsync(bucketName, objectKey, null);

                // Create content string which will contain the contents of the file itself
                string content;

                // Using StreamReader and the previously created stream
                using (StreamReader reader = new StreamReader(stream))
                {
                    // Populate the content string
                    content = reader.ReadToEnd();
                    // Close reader
                    reader.Close();
                }

                // Create new XML document and load with content
                XmlDocument doc = new XmlDocument();
                doc.LoadXml(content);
            }
            catch (Exception e)
            {
                context.Logger.LogInformation($"Error getting object {s3Event.Object.Key} from bucket {s3Event.Bucket.Name}. Make sure they exist and your bucket is in the same region as this function.");
                context.Logger.LogInformation(e.Message);
                context.Logger.LogInformation(e.StackTrace);
                throw;
            }
            // TODO
            // Parse XML
            // XmlParseUploader(doc);
        }
        else
        {
            throw new Exception("File not of correct type xml or json.");
        }

        try
        {
            var response = await this.S3Client.GetObjectMetadataAsync(s3Event.Bucket.Name, s3Event.Object.Key);
            return response.Headers.ContentType;
        }
        catch(Exception e)
        {
            context.Logger.LogInformation($"Error getting object {s3Event.Object.Key} from bucket {s3Event.Bucket.Name}. Make sure they exist and your bucket is in the same region as this function.");
            context.Logger.LogInformation(e.Message);
            context.Logger.LogInformation(e.StackTrace);
            throw;
        }
    }

    /// <summary>
    /// Parses the XML file obtained from the s3Event and inserts the data into RDS using Npgsql.
    /// </summary>
    private async Task XmlParseUploader(XmlDocument doc)
    {
        //TODO
        // Parse XML

        // Create NpgsqlConnection and upload using 
        // var cmd = new NpgsqlCommand(@"INSERT INTO ... VALUES ...")

        // cmd.ExecuteNonQuery();

        // conn.Close();
        // conn.Dispose();
    }

    /// <summary>
    /// Parses the JSON file obtained from the s3Event and inserts the data into RDS using Npgsql.
    /// </summary>
    private async Task JsonParseUploader(JsonDocument doc)
    {
        //TODO
        // Parse JSON and form SQL command
        
        // Form NpgsqlCommand
        // var cmd = new NpgsqlCommand(@"INSERT INTO ... VALUES ...");
        
        try
        {
            // Create NpgsqlConnection
            NpgsqlConnection conn = OpenConnection();

            // If connection was successful
            if (conn.State == ConnectionState.Open)
            {
                // Execute the command 
                // cmd.ExecuteNonQuery();

                // Close and dispose of the connection
                conn.Close();
                conn.Dispose();
            }
            else
            {
                Console.WriteLine("Failed to open a connection to the database. Connection state: {0}",
                    Enum.GetName(typeof(ConnectionState), conn.State));
            }

        }
        catch (NpgsqlException e)
        {
            Console.WriteLine("Npgsql Error: {0}", e.Message);
        }
        catch (Exception e)
        {
            Console.WriteLine("Error: {0}", e.Message);
        }
    }

    /// <summary>
    /// Returns an NpgsqlConnection object using predetermined endpoint parameters.
    /// </summary>
    private NpgsqlConnection OpenConnection()
    {
        // TODO
        // Update endpoint and other parameters to vax DB
        string endpoint = "mod12pginstance.cytiilpumzjl.us-east-1.rds.amazonaws.com";

        string connString = "Server=" + endpoint + ";" +
                            "port=5432;" +
                            "Database=SalesDB;" +
                            "User ID=postgres;" +
                            "password=cs455pass;" +
                            "Timeout=15";
        NpgsqlConnection conn = new NpgsqlConnection(connString);
        conn.Open();
        return conn;
    }
}
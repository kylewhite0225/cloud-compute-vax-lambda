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
using System.Xml.Serialization;

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
            throw new Exception("Event is null");
        }

        string bucketName = s3Event.Bucket.Name;
        string objectKey = s3Event.Object.Key;
        // vvv This might have to change to GetObjectTaggingRequest vvv
        // Type objectType = s3Event.Object.GetType();
        // string strType = objectType.ToString();
        
        Console.WriteLine("Bucket and object key: {0}, {1}", bucketName, objectKey);

        string[] keyArray = objectKey.Split('.');
        string strType = keyArray[1];

        // TODO
        // Get the object itself from S3 so we can parse it, we did this in Module 10

        // GET FILE using the process outlined in Module 10
        try
        {
            // Create stream to get object from S3
            Console.WriteLine("Create stream object with GetObjectStreamAsync");
            Stream stream = await S3Client.GetObjectStreamAsync(bucketName, objectKey, null);
            
            // Create jsonString string which will contain the contents of the file itself
            string fileContent;
                //"<data month=\"4\" day=\"22\" year=\"2021\"><site id=\"1000101\"><name>Bellevue Overlake Hospital</name><zipCode>98004</zipCode></site><vaccines>    <brand name=\"Pfizer\"> <total>630</total>        <firstShot>400</firstShot>        <secondtShot>230</secondtShot>    </brand>    <brand name=\"Moderna\"> <total>420</total>        <firstShot>300</firstShot>        <secondShot>120</secondShot>    </brand></vaccines></data>";

            Console.WriteLine("Use StreamReader");
            // Using StreamReader and the previously created stream
            using (StreamReader reader = new StreamReader(stream))
            {
                // Populate the jsonString string
                fileContent = reader.ReadToEnd();
                // Close reader
                reader.Close();
            }

            Console.WriteLine("Stream reader completed populating file content.");

            // If the type of the object is json or xml
            VaxRecord vaxRecord = null;
            if (strType == @"json")
            {
                Console.WriteLine("Parsing JSON");
                // Use ParseJsonVaxRecord method to create a VaxRecord object containing information from the Json
                vaxRecord = ParseJsonVaxRecord(fileContent);
            }
            else if (strType == @"xml")
            {
                Console.WriteLine("Parsing XML");
                // Use ParseXmlVaxRecord method to create a VaxRecord object containing information from the XML
                vaxRecord = ParseXmlVaxRecord(stream);
            }
            else
            {
                throw new Exception("File not of correct type xml or json.");
            }

            Console.WriteLine("Uploading to DB");
            // Use UploadVaxRecordToDB method to upload the VaxRecord object to our PostgreSQL database
            UploadVaxRecordToDB(vaxRecord).Wait();
        }
        catch (Exception e)
        {
            context.Logger.LogInformation($"Error getting object {s3Event.Object.Key} from bucket {s3Event.Bucket.Name}. Make sure they exist and your bucket is in the same region as this function.");
            context.Logger.LogInformation(e.Message);
            context.Logger.LogInformation(e.StackTrace);
            throw;
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
    private VaxRecord ParseXmlVaxRecord(Stream xmlDocument)
    {
        VaxRecord? vaxRecord = null;
        try
        {
            // Use xmlSerializer to deserialize the xml string into a VaxRecord object
            XmlRootAttribute xRoot = new XmlRootAttribute();
            xRoot.ElementName = "data";
            xRoot.IsNullable = true;
            XmlSerializer serializer = new XmlSerializer(typeof(XmlVaxRecord), xRoot);
            var parsed = serializer.Deserialize(xmlDocument);
            XmlVaxRecord xmlRecord = (XmlVaxRecord)parsed;
            vaxRecord = new VaxRecord()
            {
                date = new Date()
                {
                    year = xmlRecord.year,
                    month = xmlRecord.month,
                    day = xmlRecord.day,
                },
                site = xmlRecord.site,
            };

            var list = new List<Vaccines>();
            foreach (XmlVaccines vax in xmlRecord.vaccines)
            {
                list.Add(new Vaccines() { brand = vax.name, firstShot = vax.firstShot, secondShot = vax.secondShot, total = vax.total });
            }
            vaxRecord.vaccines = list.ToArray();
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }

        if (vaxRecord == null)
        {
            throw new Exception("Vax Record was null");
        }

        return vaxRecord;
    }

    /// <summary>
    /// Parses the JSON file obtained from the s3Event and inserts the data into RDS using Npgsql.
    /// </summary>
    private VaxRecord ParseJsonVaxRecord(string doc)
    {
        // Console.WriteLine(doc);
        VaxRecord? vaxRecord = null;
        try
        {
            Console.WriteLine("Serializing JSON");
            // Use JsonSerializerOptions object to allow ignoring case of JSON elements
            JsonSerializerOptions options = new JsonSerializerOptions();
            options.PropertyNameCaseInsensitive = true; // ignore case
            // Use JsonSerializer to deserialize the JSON string into a VaxRecord object
            vaxRecord = JsonSerializer.Deserialize<VaxRecord?>(doc, options);
        }
        catch (Exception e)
        {
            // TODO
            // Does this catch if the JSON is not well formed?
            Console.WriteLine(e.Message);
        }

        if (vaxRecord == null)
        {
            throw new InvalidOperationException("Vax Record was null.");
        }

        return vaxRecord;
    }

    private async Task UploadVaxRecordToDB(VaxRecord record)
    {
        
        // Form NpgsqlCommand to add site information to DB using the VaxRecord object members, ignore if the site already exists (on conflict do nothing)
        string command = String.Format("INSERT INTO sites VALUES ('{0}', '{1}', '{2}') ON CONFLICT DO NOTHING", record.site.id, record.site.name, record.site.zipCode);
        var cmd = new NpgsqlCommand(command);

        // Formulate date command from VaxRecord Date object
        string date = record.date.year + "-" + record.date.month + "-" + record.date.day;
        
        // Gather firstShot and secondShot counts from the array of Vaccines objects in the VaxRecord object
        int firstShot = 0;
        int secondShot = 0;
        foreach (Vaccines x in record.vaccines)
        {
            firstShot += x.firstShot;
            secondShot += x.secondShot;
        }

        // Form second NpgsqlCommand using the VaxRecord object members to add vaccine counts and date to DB
        // update if the record already exists for correcting errors
        string command2 = String.Format("INSERT INTO data VALUES ('{0}', '{1}', '{2}', '{3}') ON CONFLICT (siteid) DO UPDATE SET firstshot = EXCLUDED.firstshot, secondshot = EXCLUDED.secondshot", record.site.id, date, firstShot,
            secondShot);
        var cmd2 = new NpgsqlCommand(command2);

        Console.WriteLine("Commands formulated.");

        // Upload to DB
        try
        {
            Console.WriteLine("Opening connection");
            // Create NpgsqlConnection
            NpgsqlConnection conn = OpenConnection();

            // If connection was successful
            if (conn.State == ConnectionState.Open)
            {
                Console.WriteLine("Sending commands");
                // Execute the commands
                cmd.Connection = conn;
                await cmd.ExecuteNonQueryAsync();
                cmd2.Connection = conn;
                await cmd2.ExecuteNonQueryAsync();

                Console.WriteLine("Closing connection");
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
        Console.WriteLine("Accessing Endpoint");
        // Vaccine DB endpoint on AWS RDS
        string endpoint = "vaccinedatabase.cytiilpumzjl.us-east-1.rds.amazonaws.com";

        // Format the connection string
        string connString = "Server=" + endpoint + ";" +
                            "port=5432;" +
                            "Database=VaccineDB;" +
                            "User ID=postgres;" +
                            "password=cs455vaccine;" +
                            "Timeout=15";

        // Create NpgsqlConnection object using connection string
        NpgsqlConnection conn = new NpgsqlConnection(connString);
        conn.Open();
        Console.WriteLine("Connection open");
        return conn;
    }
}
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using System;

namespace S3Console
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            var credendialsAWS = new BasicAWSCredentials("", "");
            IAmazonS3 clientAWS = new AmazonS3Client(credendialsAWS, RegionEndpoint.USEast2);

            S3Service s3service = new S3Service(clientAWS);

            s3service.UploadAsync("bucket-name", "file-path").Wait();

            s3service.UploadAsync("bucket-name", "file-path", 100).Wait();

            s3service.DownloadAsync("bucket-name", "key-name", "destination-path", true).Wait();

            s3service.DownloadAsync("bucket-name", "destination-path", false).Wait();

        }
    }
}

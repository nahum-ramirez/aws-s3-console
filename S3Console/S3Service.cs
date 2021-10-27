using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace S3Console
{
    public class S3Service
    {
        private readonly IAmazonS3 S3Client;

        /**
         * <summary>Construye un objeto según el cliente de amazon aws s3</summary>
         * **/
        public S3Service(IAmazonS3 amazonS3Client)
        {
            S3Client = amazonS3Client;
        }

        /**
         * <summary>Cargar un archivo local a un bucket en aws s3</summary>
         * <param name="bucketName">Nombre del bucket</param>
         * <param name="filePath">Ruta completa del archivo local</param>
         * <returns>Task</returns>
         * **/
        public async Task UploadAsync(string bucketName, string filePath)
        {
            try
            {
                FileInfo file = new FileInfo(filePath);

                if (File.Exists(file.FullName))
                {
                    PutObjectRequest putRequest = new PutObjectRequest
                    {
                        BucketName = bucketName,
                        Key = file.Name,
                        FilePath = file.FullName
                    };

                    putRequest.Metadata.Add("x-amz-meta-title", Path.GetFileNameWithoutExtension(file.FullName));
                    putRequest.StreamTransferProgress += new EventHandler<StreamTransferProgressArgs>(UploadEventCallback);
                    PutObjectResponse putResponse = await S3Client.PutObjectAsync(putRequest);
                }

            }
            catch (FileNotFoundException e)
            {
                Console.WriteLine("File not found. Message:'{0}'", e.Message);
            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine("AmazonS3 exception. Message:'{0}'", e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("Unknown exception. Message:'{0}'", e.Message);
            }
        }

        /**
         * <summary>Cargar un archivo local por partes a un bucket en aws s3</summary>
         * <param name="bucketName">Nombre del bucket</param>
         * <param name="filePath">Ruta completa del archivo local</param>
         * <param name="partSize">Tamaño en Mb para cada parte</param>
         * <returns>Task</returns>
         * **/
        public async Task UploadAsync(string bucketName, string filePath, int partSize)
        {
            try
            {
                FileInfo file = new FileInfo(filePath);

                if (File.Exists(file.FullName))
                {
                    List<UploadPartResponse> uploadResponses = new List<UploadPartResponse>();

                    InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest
                    {
                        BucketName = bucketName,
                        Key = file.Name
                    };

                    initRequest.Metadata.Add("x-amz-meta-title", Path.GetFileNameWithoutExtension(file.FullName));
                    InitiateMultipartUploadResponse initResponse = await S3Client.InitiateMultipartUploadAsync(initRequest);

                    long contentLength = file.Length;
                    long partSizeL = (partSize < 5 ? 5 : partSize) * (long)Math.Pow(2, 20);

                    try
                    {
                        long filePosition = 0;
                        for (int i = 1; filePosition < contentLength; i++)
                        {
                            UploadPartRequest uploadRequest = new UploadPartRequest
                            {
                                BucketName = bucketName,
                                Key = file.Name,
                                UploadId = initResponse.UploadId,
                                PartNumber = i,
                                PartSize = partSizeL,
                                FilePosition = filePosition,
                                FilePath = filePath
                            };

                            uploadRequest.StreamTransferProgress += new EventHandler<StreamTransferProgressArgs>(UploadEventCallback);

                            uploadResponses.Add(await S3Client.UploadPartAsync(uploadRequest));

                            filePosition += partSizeL;
                        }

                        CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest
                        {
                            BucketName = bucketName,
                            Key = file.Name,
                            UploadId = initResponse.UploadId
                        };
                        completeRequest.AddPartETags(uploadResponses);

                        CompleteMultipartUploadResponse completeUploadResponse = await S3Client.CompleteMultipartUploadAsync(completeRequest);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Unknown exception. Message:'{0}'", e.Message);
                        AbortMultipartUploadRequest abortMPURequest = new AbortMultipartUploadRequest
                        {
                            BucketName = bucketName,
                            Key = file.Name,
                            UploadId = initResponse.UploadId
                        };
                        await S3Client.AbortMultipartUploadAsync(abortMPURequest);
                    }

                }
            }
            catch (FileNotFoundException e)
            {
                Console.WriteLine("File not found. Message:'{0}'", e.Message);
            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine("AmazonS3 exception. Message:'{0}'", e.Message);
            }
        }

        private static void UploadEventCallback(object sender, StreamTransferProgressArgs e)
        {
            Console.SetCursorPosition(0, Console.CursorTop);
            Console.Write("{0}", (e.TransferredBytes * 100) / e.TotalBytes);
        }

        /**
         * <summary>Descargar un archivo desde aws s3 en un directorio local</summary>
         * <param name="bucketName">Nombre del bucket</param>
         * <param name="keyName">Key del archivo en aws s3</param>
         * <param name="destinationPath">Ruta completa del directorio local</param>
         * <param name="overwrite">Sobreescribir?</param>
         * <returns>Task</returns>
         * **/
        public async Task DownloadAsync(string bucketName, string keyName, string destinationPath, bool overwrite)
        {
            try
            {
                GetObjectRequest getRequest = new GetObjectRequest
                {
                    BucketName = bucketName,
                    Key = keyName
                };

                using GetObjectResponse response = await S3Client.GetObjectAsync(getRequest);
                response.WriteObjectProgressEvent += new EventHandler<WriteObjectProgressArgs>(DownloadEventCallback);
                await response.WriteResponseStreamToFileAsync(Path.Combine(destinationPath, keyName), overwrite, default);
            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine("AmazonS3 exception. Message:'{0}'", e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("Unknown exception. Message:'{0}'", e.Message);
            }
        }

        /**
         * <summary>Descargar un directorio desde aws s3 en un directorio local</summary>
         * <param name="bucketName">Nombre del bucket</param>
         * <param name="destinationPath">Ruta completa del directorio local</param>
         * <param name="overwrite">Sobreescribir?</param>
         * <returns>Task</returns>
         * **/
        public async Task DownloadAsync(string bucketName, string destinationPath, bool overwrite)
        {
            try
            {
                ListObjectsRequest listRequest = new ListObjectsRequest
                {
                    BucketName = bucketName
                };

                ListObjectsResponse listResponse = await S3Client.ListObjectsAsync(listRequest).ConfigureAwait(false);

                foreach (S3Object item in listResponse.S3Objects)
                {
                    string keyPath = Path.Combine(item.Key.Split("/"));
                    string filePath = Path.Combine(destinationPath, keyPath);
                    if (Utils.IsPathFile(filePath))
                    {
                        try
                        {
                            GetObjectRequest getRequest = new GetObjectRequest
                            {
                                BucketName = item.BucketName,
                                Key = item.Key
                            };

                            Utils.CreateFileDirectory(filePath);

                            using GetObjectResponse response = await S3Client.GetObjectAsync(getRequest);
                            response.WriteObjectProgressEvent += new EventHandler<WriteObjectProgressArgs>(DownloadEventCallback);
                            await response.WriteResponseStreamToFileAsync(filePath, overwrite, default);
                        }
                        catch (AmazonS3Exception e)
                        {
                            Console.WriteLine("AmazonS3 exception. Message:'{0}'", e.Message);
                            continue;
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("Unknown exception. Message:'{0}'", e.Message);
                            continue;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Unknown exception. Message:'{0}'", e.Message);
            }
        }

        private static void DownloadEventCallback(object sender, WriteObjectProgressArgs e)
        {
            Console.SetCursorPosition(0, Console.CursorTop);
            Console.Write("{0}", (e.TransferredBytes * 100) / e.TotalBytes);
        }

    }
}

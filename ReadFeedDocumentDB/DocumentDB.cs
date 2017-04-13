using System;
using System.Linq;
using System.Threading.Tasks;

// Needed for connecting to DocumentDB account
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using Microsoft.Azure.Documents.Linq;
using System.Threading;

namespace ReadFeedDocumentDB
{
    public class ECITLOGGING_PARTITION
    {
        //[JsonProperty(PropertyName = "id")]
        public UInt64 EventLogID { get; set; }
        public string ApplicationName { get; set; }
        public string EnvironmentName { get; set; }
        public string ActivityID { get; set; }
        public UInt64 EventID { get; set; }
        public string Severity { get; set; }
        public string Title { get; set; }
        public string Timestamp { get; set; }
        public string MachineName { get; set; }
        public string Method { get; set; }
        public string Message { get; set; }
        public string SerializedObject { get; set; }
        public string CreatedDate { get; set; }
        public string CreatedBy { get; set; }
        public string LastModifiedDate { get; set; }
        public string LastModifiedBy { get; set; }
        public string id { get; set; }
        public string _rid { get; set; }
        public string _self { get; set; }
        public string _etag { get; set; }
        public string _attachments { get; set; }
        public UInt64 _ts { get; set; }

        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }
    }

    public class DocumentDB
    {
        // Private member varibles
        private const string EndpointURIString  = "https://explorems.documents.azure.com:443/";
        private const string PrimaryKey         = "5NLmGrcORlqlqRVcqvmwSLCDSqL7m0cWTM5qeGs5qFwczBitinnC3VWUDw8TD4xq2qw5XQx6EBhejFOAGhffRw==";
        private const string CollectionName     = "ECITLOGGING_PARTITION"; // "ECITLogging2"
        private const string DatabaseName       = "Agreement";
        private const string PartitionKey       = "EventLogID";
        private DocumentClient client;

        private static void TESTCONCATENATION()
        {
            string first = "2016-08-02 22:46:04.820";
            string second = "2016-08-02 23:46:04.820";
            int compared = first.CompareTo(second);
            if (compared  < 0)
            {
                Console.WriteLine("First is less than second");
            }
            else
            {
                Console.WriteLine("First is NOT less than second");
            }
            //"2016-08-02 22:46:04.820" and '2016-08-02 23:46:04.820'
            const int sLen = 30, Loops = 5000 *5;
            DateTime sTime, eTime;
            int i;
            string sSource = new String('X', sLen);
            string sDest = "";

            // 
            // Time string concatenation.
            // 
            sTime = DateTime.Now;
            for (i = 0; i < Loops; i++) sDest += sSource;
            eTime = DateTime.Now;
            Console.WriteLine("Concatenation took " + (eTime - sTime).TotalSeconds + " seconds.");
            // 
            // Time StringBuilder.
            // 
            sTime = DateTime.Now;
            System.Text.StringBuilder sb = new System.Text.StringBuilder((int)(sLen * Loops * 1.1));
            for (i = 0; i < Loops; i++) sb.Append(sSource);
            sDest = sb.ToString();
            eTime = DateTime.Now;
            Console.WriteLine("String Builder took " + (eTime - sTime).TotalSeconds + " seconds.");
            // 
            // Make the console window stay open
            // so that you can see the results when running from the IDE.
            // 
            Console.WriteLine();
            Console.Write("Press Enter to finish ... ");
            Console.Read();
        }

        static void Main(string[] args)
        {

            TESTCONCATENATION();

            //SQLtoNoSQLHelper sqlHelper = new SQLtoNoSQLHelper();
            //sqlHelper.ConvertDBtoSQL("Hello World!");
            string sqlQuery = "SELECT c.EventLogID FROM c " + 
                              "WHERE c.ApplicationName='QuoteServices: ExceptionService' " +
                              "AND c.EventID=1010 " +
                              "AND c.EnvironmentName='Production'";// WHERE c.Severity='Verbose' ORDER BY c.ApplicationName";

            try
            {
                DocumentDB p = new DocumentDB();
                p.ExecuteSqlQuery(sqlQuery).Wait();
                //p.QueryAllDataInCollection.Wait();
            }
            catch (DocumentClientException de)
            {
                Exception baseException = de.GetBaseException();
                Console.WriteLine("{0} error occurred: {1}, Message: {2}", de.StatusCode, de.Message, baseException.Message);
            }
            catch (Exception e)
            {
                Exception baseException = e.GetBaseException();
                Console.WriteLine("Error: {0}, Message: {1}", e.Message, baseException.Message);
            }
            finally
            {
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }
            return;
        }

        private async Task QueryAllDataInCollection()
        {

            //ExecuteSimpleQuery(DatabaseName, CollectionName);
            this.client     = new DocumentClient(new Uri(EndpointURIString), PrimaryKey);
            var database    = (await this.client.ReadDatabaseFeedAsync()).Single(d => d.Id == DatabaseName);
            var collection  = (await this.client.ReadDocumentCollectionFeedAsync(database.CollectionsLink)).Single(c => c.Id == CollectionName);

            //ExecuteSimpleQuery(DatabaseName, CollectionName, collection.SelfLink);
            // Read the data in chunks say 100 documents/rows at a time
            string continutationString = string.Empty;
            FeedResponse<dynamic> docs;
            int docCount = 0;
            do
            {
                // Default read of 100 items or 1MB if MaxItemCount is not specified.
                docs = (await this.client.ReadDocumentFeedAsync(collection.DocumentsLink, new FeedOptions { RequestContinuation = continutationString, MaxItemCount = 1000}));
                docCount += docs.Count;
                continutationString = docs.ResponseContinuation;

                // Print out all the documents if we find any
                if(docs.Count > 0)
                {
                    if(!docs.ResponseContinuation.Contains("PKRID:7"))
                    {
                        Console.WriteLine("This is a different partition?");
                    }
                    foreach (var document in docs)
                    {
                        Console.WriteLine(document.ToString());
                    }
                }
            } while (!string.IsNullOrEmpty(continutationString));

            // Read the full data (even if it's very large say 10GB)
            //var docs = client.CreateDocumentQuery(collection.DocumentsLink).ToList();
            //foreach (var document in docs)
            //{
            //    Console.WriteLine(document.ToString());
            //}
            //IQueryable<dynamic> results = client.CreateDocumentQuery(EndpointURIString, "SELECT * FROM ECITLogging");
            //var things      = await this.client.ReadDocumentCollectionFeedAsync(EndpointURIString, new FeedOptions { MaxItemCount = 10 });
            //var docs        = await this.client.ReadDocumentFeedAsync(EndpointURIString, new FeedOptions {MaxItemCount=10});
            //foreach (var d in docs)
            //{
            //    Console.WriteLine(d);
            //}
        }

        // We can now execute specific "sql" query across a Database.Collection which is partitioned!
        //
        // 429: "Request rate is large" indicates that the application has exceeded the
        // provisioned RU quota and should retry the request after some time interval
        // 503: Service unavailable
        //
        // Clients may not catch 429 since .NET SDK will handle it implicitly and retry the request when exceeding RU's
        // unless you have multiple concurrent clients: https://azure.microsoft.com/en-us/blog/performance-tips-for-azure-documentdb-part-2/
        //
        // What to do if 429 or 503 happen?: https://social.msdn.microsoft.com/Forums/sqlserver/en-US/8edaa6bf-a141-4363-b280-0964d0525129/request-rate-is-too-large-azure-document-db?forum=AzureDocumentDB
        // HTTP status codes: https://msdn.microsoft.com/en-us/library/azure/dn783364.aspx
        public async Task ExecuteSqlQuery(string sqlQuery)
        {
            this.client                             = new DocumentClient(new Uri(EndpointURIString), PrimaryKey);
            FeedOptions             feedOptions     = new FeedOptions{MaxItemCount = -1, EnableCrossPartitionQuery = true};
            var                     collectionUri   = UriFactory.CreateDocumentCollectionUri(DatabaseName, CollectionName);
            IDocumentQuery<dynamic> docQuery        = this.client.CreateDocumentQuery(collectionUri, sqlQuery, feedOptions).AsDocumentQuery();
            UInt64                  documentCount   = 0;

            // Make sure we can continue querying the partitions
            while(docQuery != null && docQuery.HasMoreResults)
            {
                // Print out all the documents in the Collection
                try
                {
                    var results     = (await docQuery.ExecuteNextAsync());
                    documentCount   += Convert.ToUInt64(results.Count);
                    //foreach (dynamic result in results)
                    //{
                    //    Console.WriteLine("\tRead {0}", result.ToString());
                    //}

                    Console.WriteLine("\tTotal Documents {0}", documentCount);
                }
                catch (DocumentClientException exception)
                {
                    if (exception != null &&
                        exception.StatusCode != null &&
                        ((int)exception.StatusCode == 429 || (int)exception.StatusCode == 503))
                    {
                        Console.WriteLine("Query failed with status code {0} retrying after {1} seconds.", (int)exception.StatusCode, exception.RetryAfter.Seconds);
                        Thread.Sleep(exception.RetryAfter);
                    }
                    else
                        throw;
                }
                catch (AggregateException exception)
                {
                    if (exception.InnerException.GetType() == typeof(DocumentClientException))
                    {
                        var docException = exception.InnerException as DocumentClientException;
                        if ((int)docException.StatusCode == 429 ||
                           (int)docException.StatusCode == 503)
                        {
                            Console.WriteLine("Query failed with status code {0} retrying after {1} seconds.", (int)docException.StatusCode, docException.RetryAfter.Seconds);
                            Thread.Sleep(docException.RetryAfter);
                        }
                        else
                            throw;
                    }
                }
                catch (Exception exception)
                {
                    Console.WriteLine("Query failed with status code {0}", exception.StackTrace);
                }
            }
        }
    }
}

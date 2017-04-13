using System.Web.Mvc;
using SQLtoNoSQL.Models;
//using ReadFeedDocumentDB;
using System;
using System.Linq;
using System.Threading.Tasks;

// Needed for connecting to DocumentDB account
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using Microsoft.Azure.Documents.Linq;
using System.Threading;
using System.Text;

namespace SQLtoNoSQL.Controllers
{
    public class HomeController : Controller
    {
        // Private member varibles
        private const string EndpointURIString  = "https://explorems.documents.azure.com:443/";
        private const string PrimaryKey         = "5NLmGrcORlqlqRVcqvmwSLCDSqL7m0cWTM5qeGs5qFwczBitinnC3VWUDw8TD4xq2qw5XQx6EBhejFOAGhffRw==";
        private const string CollectionName     = "ECITLOGGING_PARTITION";
        private const string DatabaseName       = "Agreement";
        private const string PartitionKey       = "EventLogID";
        private DocumentClient client;

        public ActionResult Index()
        {
            return View();
        }

        public ActionResult About()
        {
            ViewBag.Message = "Trying to perform some SQL and DocumentDB experiments!";

            return View();
        }

        public ActionResult Contact()
        {
            ViewBag.Message = "Contact us with questions or concerns!";

            return View();
        }

        [HttpGet]
        public ActionResult SQLtoNoSQL()
        {
            /* TODO TEST */
            //SQLtoNoSQLHelper sqlHelper = new SQLtoNoSQLHelper();
            //sqlHelper.ConvertDBtoSQL("Hello World!");
            /* TODO TEST */

            ViewBag.Message             = "This is the SQLtoNoSQL page.";

            SQLtoNoSQLViewModel model = new SQLtoNoSQLViewModel("SELECT * FROM c", "");
            return View(model);
        }

        //  THESE ARE THE STEPS WE HAVE TO PERFORM:
        //  (1) Convert SQL query(or subset of the constraints) into DocumentDB query
        //  (2) Execute DocumentDB query
        //  (3) Infer schema of SQL temp table(column name and type)
        //  (4) Create SQL temp table(SQLTT)
        //  (5) Take result of(2) and insert into SQLTT
        //  (6) Run original SQL query on SQLTT
       [HttpPost]
        public async Task<ActionResult> SQLtoNoSQL(SQLtoNoSQLViewModel model, bool paging = true)
        {
            // Gather the data from the SQL query!
            bool exceptionHappened = false;
            try
            {
                SQLtoNoSQLHelper sqlHelper = new SQLtoNoSQLHelper();
                // TODO: (1)
                sqlHelper.convertSQLQueryToDocDBQuery(model);

                // (2)
                await ExecuteDocDBQuery(model, paging);

                // (3)
                // TODO: Temporary for now.. just pass the queryResult to the mega function
                sqlHelper.executeSQLQuery(model);
            }
            catch (DocumentClientException de)
            {
                Exception baseException = de.GetBaseException();
                model.QueryResult = String.Format("{0} error occurred: {1}, Message: {2}", de.StatusCode, de.Message, baseException.Message);
                exceptionHappened = true;
            }
            catch (Exception e)
            {
                Exception baseException = e.GetBaseException();
                model.QueryResult = String.Format("Error: {0}, Message: {1}", e.Message, baseException.Message);
                exceptionHappened = true;
            } 
            finally // TODO: Maybe do something here?
            {
            }

            // Update the POST response
            ViewBag.Message = "This is the SQLtoNoSQL page.";

            // Tell user no results or update formatting if results are found
            // implicit else: Exception happened
            if (model.QueryResult.Equals(""))
            {
                model.QueryResult = "No Results were found for your query!";
            }
            else if (!exceptionHappened)
            {
                model.QueryResult = model.QueryResult.Replace("}", "}\n");
                // Allows user to page the results, and load them on demand after query
                ViewBag.LoadMoreResults = true;
            }

            return View(model);
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
        private async Task ExecuteDocDBQuery(SQLtoNoSQLViewModel model, bool paging)
        {
            this.client                             = new DocumentClient(new Uri(EndpointURIString), PrimaryKey);
            FeedOptions             feedOptions     = new FeedOptions { MaxItemCount = (paging ? 3 : -1), EnableCrossPartitionQuery = true, RequestContinuation = (string)Session["ContinuationString"] };
            var                     collectionUri   = UriFactory.CreateDocumentCollectionUri(DatabaseName, CollectionName);
            IDocumentQuery<dynamic> docQuery        = this.client.CreateDocumentQuery(collectionUri, model.DocDBQuery, feedOptions).AsDocumentQuery();
            UInt64                  documentCount   = 0;
            StringBuilder           sb              = new StringBuilder();
            FeedResponse<dynamic>   results;

            // Make sure we can continue querying the partitions
            do
            {
                // Print out all the documents in the Collection
                try
                {
                    // TODO: To disable paging, add while loop on docQuery.HasMoreResults, need to add UI option too...
                    do
                    {
                        // TODO: Return document count to the View?
                        results         = await docQuery.ExecuteNextAsync();
                        documentCount   += Convert.ToUInt64(results.Count);
                        foreach (dynamic result in results)
                        {
                            sb.Append(result);
                        }
                    } while (docQuery.HasMoreResults && !paging);

                    model.QueryResult = sb.ToString();
                    // This is the loading of results on demand to the user
                    Session["ContinuationString"] = results.ResponseContinuation;
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
            } while (false);//(docQuery != null && docQuery.HasMoreResults);

            //return null;
        }

    }
}
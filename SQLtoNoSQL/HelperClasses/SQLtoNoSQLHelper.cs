using SQLtoNoSQL.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Text.RegularExpressions;
using SqlTokenizerNameSpace;
using SQLtoNoSQL.HelperClasses;

namespace SQLtoNoSQL.Controllers
{
    public enum SQLTypes
    {
        E_nvarchar = 0, // string
        E_int,          // int
        E_bigint,       // int64
        E_datetime      // date and time
    }

    public class SQLtoNoSQLHelper
    {
        private class SQLSchemaData
        {
            public string   ColumnName  { get; set; }
            public string   ColumnValue { get; set; }
            public SQLTypes ColumnType  { get; set; }

            public SQLSchemaData()
            {
                ColumnName  = "";
                ColumnValue = "";
                ColumnType  = SQLTypes.E_bigint;
            }
            public SQLSchemaData(string newColumnName, string newColumnValue)//SQLTypes newColumnType)
            {

                ColumnType  = SQLTypes.E_bigint;
                ColumnName  = newColumnName;
                ColumnValue = newColumnValue;

                // logic to match the string to value
                if (Regex.IsMatch(newColumnValue, @"^\d+$"))
                {
                    // This is a bigint or int, but use bigger data type
                    // Assume bigint may help us avoid data size issues.
                    ColumnType  = SQLTypes.E_bigint;
                    ColumnValue = newColumnValue;
                }
                //else if (Regex.IsMatch(newColumnValue, @"""\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\.\d*"""))
                //{
                //    // This is a datetime
                //    ColumnType  = SQLTypes.E_datetime;
                //    ColumnValue = newColumnValue.Substring(1, newColumnValue.LastIndexOf("\"") - 1);
                //}
                else if (Regex.IsMatch(newColumnValue, @""".*"""))
                {
                    // This is a string
                    ColumnType  = SQLTypes.E_nvarchar;
                    ColumnValue = "'" + newColumnValue.Substring(1, newColumnValue.LastIndexOf("\"") - 1) + "'";
                }
            }
        }

        private class DocumentDBEntry
        {
            public List<SQLSchemaData> SchemaList { get; set; }

            public DocumentDBEntry()
            {
                SchemaList = new List<SQLSchemaData>();
            }
            public DocumentDBEntry(List<SQLSchemaData> newSchemaList)
            {
                SchemaList = new List<SQLSchemaData>(newSchemaList);
            }
        }

        private const string    DatabaseName        = "TempDB";
        private const string    ConnectionString    = "Data Source=(LocalDB)\\MSSQLLocalDB;AttachDbFilename=C:\\Users\\brruoff\\Documents\\Visual Studio 2015\\Projects\\ReadFeedDocumentDB\\SQLtoNoSQL\\App_Data\\TempDB.mdf;Integrated Security=True";
        private const string    TempTableName       = "#TempTable";
        private SqlConnection   SqlConnectionObject;

        private string ExecuteSQLCommand(string sqlCommandString)
        {
            string      exceptionString = "";
            int         rowsAffected    = 0;
            SqlCommand  sqlCommand;

            try
            {
                // TODO: maybe return rowsAffected?
                sqlCommand      = new SqlCommand(sqlCommandString, SqlConnectionObject);
                rowsAffected    = sqlCommand.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                exceptionString = String.Format("Error: {0}, Message: {1}", e.Message, e.GetBaseException().Message);
            }
            
            return exceptionString;
        }

        // Step (3)
        // Determine SQL schema given a single row from document DB
        private List<SQLSchemaData> BuildSchemaList(string docDBJsonResults)
        {
            List<SQLSchemaData> listToReturn        = new List<SQLSchemaData>();
            string              singleDocument      = docDBJsonResults.Substring(1, docDBJsonResults.IndexOf('}') - 1).Trim();
            string[]            keyValuePairArray   = Regex.Split(singleDocument, "\r\n|\r|\n");
            
            // Create ColumnName,ColumnType pairs to build temp SQL table
            for(int i = 0; i < keyValuePairArray.Length; ++i)
            {
                string keyValueElement  = keyValuePairArray[i];
                string columnName       = keyValueElement.Trim();
                columnName              = columnName.Substring(1, columnName.IndexOf(':') - 2);
                string value            = keyValueElement.Substring(keyValueElement.IndexOf(':') + 2);
                
                // Only do this if we aren't on last keyValue pair (last one doesn't have comma)
                if (i + 1 != keyValuePairArray.Length)
                {
                    value = value.Substring(0, value.LastIndexOf(','));
                }

                // This will add the value and also infer the type
                listToReturn.Add(new SQLSchemaData(columnName, value));
            }
            return listToReturn;
        }
        
        // Step (4-5)
        // Converts documentDB (JSON) string to SQL table
        // @return: Table name
        private string CreateTempSQLTableAndInsertJSON(List<SQLSchemaData> schemaList, string docDBJsonResults)
        {
            StringBuilder           sbCreate        = new StringBuilder("CREATE TABLE " + TempTableName + " (");
            StringBuilder           sbInsert        = new StringBuilder("INSERT INTO " + TempTableName + " (");
            List<DocumentDBEntry>   documentList    = new List<DocumentDBEntry>();
            string[]                rawDocumentList = Regex.Split(docDBJsonResults, "(?={)");
            string                  returnValue     = "";
            DataTable               dataTable       = new DataTable();
            SqlBulkCopy             sqlBulkCopy     = new SqlBulkCopy(SqlConnectionObject,
                                                                      SqlBulkCopyOptions.TableLock |
                                                                      SqlBulkCopyOptions.FireTriggers |
                                                                      SqlBulkCopyOptions.UseInternalTransaction,
                                                                      null);
            sqlBulkCopy.DestinationTableName        = TempTableName;


            // (3.5)
            // Construct the string required to create the SQL table
            //foreach (SQLSchemaData schemaElement in schemaList)
            for (int i = 0; i < schemaList.Count; ++i)
            {
                sbCreate.Append("[").Append(schemaList[i].ColumnName).Append("]");
                switch(schemaList[i].ColumnType)
                {
                    case (SQLTypes.E_bigint):
                        sbCreate.Append("[bigint]");
                        dataTable.Columns.Add(schemaList[i].ColumnName, typeof(Int64));
                        break;
                    case (SQLTypes.E_nvarchar):
                        sbCreate.Append("[nvarchar](512) NOT NULL");
                        dataTable.Columns.Add(schemaList[i].ColumnName, typeof(string));
                        break;
                    case (SQLTypes.E_datetime):
                        sbCreate.Append("[datetime] NULL");
                        dataTable.Columns.Add(schemaList[i].ColumnName, typeof(string));//typeof(DateTime));
                        break;
                    default:
                        break;
                }
                sbInsert.Append(schemaList[i].ColumnName);

                
                if (i + 1 != schemaList.Count)    // Add comma
                {
                    sbCreate.Append(", ");
                    sbInsert.Append(", ");
                }
                else                            // Close the statement
                {
                    sbCreate.Append(")");
                    sbInsert.Append(") VALUES ");
                }
            }

            // Create Temporary table
            SqlConnectionObject.Open();
            returnValue = ExecuteSQLCommand(sbCreate.ToString());

            // (4.5)
            // Convert docDBJsonResults to a structure we can easily parse
            // Ignore the first entry which is empty
            for (int i = 1; i < rawDocumentList.Length; ++i)
            {
                DocumentDBEntry document = new DocumentDBEntry(BuildSchemaList(rawDocumentList[i]));
                documentList.Add(document);
            }

            // (4.75)
            // Construct SQL string needed to insert data!
            // Need to use BulkCopy here otherwise we will run into:
            //
            // Error: The number of row value expressions in the INSERT
            //        statement exceeds the maximum allowed number of 1000 row values.
            foreach (var document in documentList)
            {
                int i = 0;
                DataRow row = dataTable.NewRow();
                foreach (var schema in document.SchemaList)
                {
                    row[i++] = schema.ColumnValue;
                }
                dataTable.Rows.Add(row);
            }
            // (5)
            // Take results and insert into SQL table
            sqlBulkCopy.WriteToServer(dataTable);

            return returnValue;
        }

        // Step (6)
        // Actually runs the original SQL query against the temp table we have created
        private void QuerySQLTable(SQLtoNoSQLViewModel model, List<SQLSchemaData> schemaList)
        {
            try
            {
                // Query the table for values!
                // TODO: Modify the real query to use our TempTable
                // Modify the SQL command to use the TempTable
                for (int i = 0; i < schemaList.Count; ++i)
                {
                    string columnName   = schemaList[i].ColumnName;
                    string original     = model.TableName + "." + columnName;
                    string replacement  = TempTableName + "." + columnName;

                    model.SqlQuery = model.SqlQuery.Replace(model.SqlQuery.Contains(original) ? original : columnName, replacement);
                }
                model.SqlQuery = model.SqlQuery.Replace(model.TableName + ".", TempTableName + ".");
                model.SqlQuery = model.SqlQuery.Replace("FROM " + model.TableName, "FROM " + TempTableName);

                SqlCommand sqlCommand = new SqlCommand(model.SqlQuery, SqlConnectionObject);//"SELECT * FROM " + TempTableName, SqlConnectionObject);
                SqlDataReader   sqlReader   = sqlCommand.ExecuteReader();
                StringBuilder   sb          = new StringBuilder();

                while (sqlReader.Read())
                {
                    sb.Append("{\n");
                    for (int i = 0; i < sqlReader.FieldCount; ++i)
                    {
                        sb.Append("\t").Append(sqlReader.GetName(i)).Append(":").Append(sqlReader.GetValue(i).ToString());
                        
                        // Don't need comma for last element
                        if (i + 1 != sqlReader.FieldCount)
                        {
                            sb.Append(",\n");
                        }
                    }
                    sb.Append("\n}");
                }
                model.QueryResult = sb.ToString();
            }
            catch (Exception e)
            {
                Exception baseException = e.GetBaseException();
                string exceptionString = String.Format("Error: {0}, Message: {1}", e.Message, baseException.Message);
            }
        }
        
        // Default Constructor
        public SQLtoNoSQLHelper()
        {
            SqlConnectionObject = new SqlConnection(ConnectionString);
        }

        public void convertSQLQueryToDocDBQuery(SQLtoNoSQLViewModel model)
        {
            //SqlTokenizer sqlTokenizer   = new SqlTokenizer();
            //SqlParsedInfo sqlParsedInfo = new SqlParsedInfo();// sqlTokenizer.BradFunction(model.SqlQuery);

            SqlTokenizer2 tokenizer     = new SqlTokenizer2();
            string docDBQuery = tokenizer.convertSQLToDocDB(model);
            //StringBuilder sb            = new StringBuilder();

            //// "SELECT "
            //sb.Append(sqlParsedInfo.OperationName).Append(" ");
            //if (sqlParsedInfo.ColumnsInSelectClause.Count == 0)
            //{
            //    sb.Append("*");
            //}
            //else for (int i = 0; i < sqlParsedInfo.ColumnsInSelectClause.Count; ++i)
            //{
            //    // These come back as TableName.Identifier,
            //    sb.Append(sqlParsedInfo.ColumnsInSelectClause[i]);
            //    if (i + 1 != sqlParsedInfo.ColumnsInSelectClause.Count)
            //        sb.Append(", ");
            //}
            //sb.Append(" FROM ").Append(sqlParsedInfo.TableName).Append(" ");

            //// WHERE ... AND ... OR
            //if(!sqlParsedInfo.WhereClause.Equals("WHERE "))
            //    sb.Append(sqlParsedInfo.WhereClause);

            // Update the docDB query
            //model.DocDBQuery = sb.ToString();
            model.DocDBQuery = docDBQuery;
        }

        // For now this function will do steps (3-6)
        // model.Query = Original SQL query
        // model.QueryResult = DocumentDB results running slimmer version of model.Query
        public string executeSQLQuery(SQLtoNoSQLViewModel model)
        {
            // DocumentDB had no results, no need to convert anything
            if (model.QueryResult.Equals(""))
            {
                goto exit;
            }
            // (3)
            List<SQLSchemaData> schemaList = BuildSchemaList(model.QueryResult);

            // (4-5)
            string result = CreateTempSQLTableAndInsertJSON(schemaList, model.QueryResult);

            // (6)
            QuerySQLTable(model, schemaList);
            SqlConnectionObject.Close();
exit:
            return "";
        }

    } // end class
} // end namespace
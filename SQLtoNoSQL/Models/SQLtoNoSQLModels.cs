using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace SQLtoNoSQL.Models
{
    public class SQLtoNoSQLViewModel
    {
        public string DocDBQuery { get; set; }

        [Required]
        [Display(Name = "SqlQuery")]
        public string SqlQuery { get; set; }

        [Display(Name = "QueryResult")]
        public string QueryResult { get; set; }

        public string TableName { get; set; }

        // Default Constructor
        public SQLtoNoSQLViewModel()
        {
            SqlQuery    = "SELECT * FROM c";
            QueryResult = "";
            DocDBQuery  = "";
        }

        public SQLtoNoSQLViewModel(string newSqlQuery, string newQueryResult)
        {
            SqlQuery    = newSqlQuery;
            QueryResult = newQueryResult;
            DocDBQuery  = "";
        }
    }
}
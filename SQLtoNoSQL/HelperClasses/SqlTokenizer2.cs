using SQLtoNoSQL.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Web;

namespace SQLtoNoSQL.HelperClasses
{
    public class SqlTokenizer2
    {
        public string TableName { get; set; }
        public List<string> Columns { get; set; }
        private int IndexOfFrom { get; set; }
        private int IndexOfWhere { get; set; }

        // private variables
        private string[] KeyWords = new[]
        {
            "AND",
            "OR",
            "LIKE",
            "BETWEEN",
            "<",
            ">",
            "<=",
            ">=",
            "=",
            "!="
        };
        // Default constructor
        public SqlTokenizer2()
        {
            IndexOfFrom = -1;
            TableName   = "";
            Columns     = new List<string>();
        }

        private string FindTableName(string[] sqlTokenArray)
        {
            int     indexOfAs       = -1;
            string  tableName       = "";

            for (int i = 0; i < sqlTokenArray.Count(); ++i)
            {
                // We found FROM
                if (sqlTokenArray[i].ToUpper().Equals("FROM"))
                {
                    IndexOfFrom = i;
                }
                // TODO?
                // We found AS: Assume none of columns are aliased
                if (sqlTokenArray[i].ToUpper().Equals("AS"))
                {
                    // AS came before FROM (Alias column name)
                    // Or FROM has yet to be assigned
                    if (indexOfAs < IndexOfFrom || IndexOfFrom == -1)
                    {
                        indexOfAs = i;
                        continue;
                    }
                    else
                    {
                        break;
                    }
                }
                // We found WHERE = exit!
                if (sqlTokenArray[i].ToUpper().Equals("WHERE"))
                {
                    IndexOfWhere = i;
                    break;
                }
            }

            // AS never found (no table alias), or it was found for a column (AS < FROM)
            if ((indexOfAs == -1 || indexOfAs < IndexOfFrom) && IndexOfFrom + 1 < sqlTokenArray.Count())
            {
                tableName = sqlTokenArray[IndexOfFrom + 1];
            }
            // AS found, it's assigned, it's after FROM
            else if (indexOfAs != -1 && indexOfAs > IndexOfFrom && indexOfAs + 1 < sqlTokenArray.Count())
            {
                tableName = sqlTokenArray[indexOfAs + 1];
            }

            return tableName;
        }

        // TODO: How to handle Column Aliasing?
        private void FindColumns(string[] sqlTokenArray)
        {
            string[] columns = sqlTokenArray[IndexOfFrom - 1].Split(',');
            foreach (string column in columns)
            {
                if (!column.ToUpper().Contains("COUNT(") && !column.Equals("*"))
                {
                    Columns.Add(!column.Contains(".") ? (TableName + "." + column) : column);
                }
            }
        }

        private string FindNextKeyWord(string[] sqlTokenArray, ref int startIdx)
        {
            string returnValue = "";
            for (; startIdx < sqlTokenArray.Count(); ++startIdx)
            {
                string keyWord = sqlTokenArray[startIdx].ToUpper();
                if (KeyWords.Contains(keyWord))
                {
                    returnValue = keyWord;
                    break;
                }
            }
            return returnValue;
        }

        // Handle strings like:
        // %searchForMe% ==> CONTAINS
        // %searchForMe ==> ENDSWITH
        // searchForMe% ==> BEGINSWITH
        private void HandleLikeClause(string[] sqlTokenArray, ref int IndexOfLike, StringBuilder sb)
        {
            string docDBFunctionToUse   = "CONTAINS(";
            string searchString         = sqlTokenArray[IndexOfLike + 1];

            // ENDS WITH
            if (searchString.StartsWith("%") && !searchString.EndsWith("%"))
            {
                docDBFunctionToUse = "ENDSWITH(";
            }
            // STARTS WITH
            if (!searchString.StartsWith("%") && searchString.EndsWith("%"))
            {
                docDBFunctionToUse = "STARTSWITH(";
            }

            sb.Append(docDBFunctionToUse);
            sb.Append(sqlTokenArray[IndexOfLike - 1]);
            sb.Append(", ");

            // Remove % from search string
            searchString = searchString.Replace("%", "");
            sb.Append(searchString);
            sb.Append(")");
            IndexOfLike += 1;
            return;
        }
        private void HandleBetweenOperator(string[] sqlTokenArray, ref int IndexOfBetween, StringBuilder sb)
        {
            sb.Append("(");
            sb.Append(sqlTokenArray[IndexOfBetween - 1]);
            sb.Append(" BETWEEN ");
            sb.Append(sqlTokenArray[IndexOfBetween + 1]);
            sb.Append(" AND ");
            sb.Append(sqlTokenArray[IndexOfBetween + 3]);
            sb.Append(")");
            IndexOfBetween += 3;
        }

        // <, >, >=, <=, =, !=
        private void HandleComparatorOperator(string[] sqlTokenArray, ref int indexOfComparator, StringBuilder sb)
        {
            sb.Append(sqlTokenArray[indexOfComparator - 1]);
            sb.Append(sqlTokenArray[indexOfComparator]);
            sb.Append(sqlTokenArray[indexOfComparator + 1]);
            indexOfComparator += 1;
        }

        private void BuildQueryAfterWhereClause(string[] sqlTokenArray, StringBuilder sb)
        {
            // TODO: Handle OOB errors
            for (int i = IndexOfWhere; i < sqlTokenArray.Count(); ++i)
            {
                string keyWord = FindNextKeyWord(sqlTokenArray, ref i);
                switch (keyWord)
                {
                    case "LIKE":
                        HandleLikeClause(sqlTokenArray, ref i, sb);
                        break;
                    case "BETWEEN":
                        HandleBetweenOperator(sqlTokenArray, ref i, sb);
                        break;
                    case "<":
                    case ">":
                    case "=":
                    case "<=":
                    case ">=":
                    case "!=":
                        HandleComparatorOperator(sqlTokenArray, ref i, sb);
                        break;
                    case "AND":
                    case "OR":
                    default: // ignore this ID for now, only look for keywords
                        sb.Append(" ").Append(keyWord).Append(" ");
                        break;
                }
            }
        }

        private string FormatSqlQueryString(string sqlQuery)
        {
            sqlQuery = sqlQuery.Trim();
            sqlQuery = sqlQuery.Replace(", ", ","); // Convert ", " to ","

            // Add spaces between comparators
            sqlQuery = sqlQuery.Replace("<=", " <= ");
            sqlQuery = sqlQuery.Replace(">=", " >= ");
            sqlQuery = sqlQuery.Replace("!=", " != ");

            // Replace Single "<" with " < ", don't match "<=" this will yield " < = "
            Regex r = new Regex(@"<(?!=)");
            sqlQuery = r.Replace(sqlQuery, " < ");

            // Replace Single ">" with " > ", don't match ">=" this will yield " > = "
            r = new Regex(@">(?!=)");
            sqlQuery = r.Replace(sqlQuery, " > ");
            
            // Replace Single "=" with " = ", don't match "!=" or ">=" or "<=" this will yield " ! = "... etc.
            r = new Regex(@"(?<![!<>])=");
            sqlQuery = r.Replace(sqlQuery, " = ");

            // Remove spaces between strings/words, and remove trailing/ending spaces
            r = new Regex(@"[ ]{2,}"); // Remove spaces greater than two
            sqlQuery = r.Replace(sqlQuery, @" ");
            return sqlQuery;
        }


        // Converts standalone columns into TableName.Col format that docDB requires.
        private string ReplaceColumnsWithAlias(string docDBQuery)
        {
            foreach (string col in Columns)
            {
                int indexOfDot = col.IndexOf('.');
                if (indexOfDot != -1)
                {
                    docDBQuery = Regex.Replace(docDBQuery, @"(?<!\.)" + col.Substring(indexOfDot + 1), col);
                }
            }
            return docDBQuery;
        }

        public string convertSQLToDocDB(SQLtoNoSQLViewModel model)//string sqlQuery)
        {
            StringBuilder sb            = new StringBuilder("");
            model.SqlQuery              = FormatSqlQueryString(model.SqlQuery);

            // Good reference for what I was trying to do:
            // http://stackoverflow.com/questions/4780728/regex-split-string-preserving-quotes/4780801#4780801
            // Split on spaces/newline/carriage return, but don't split internal strings that have spaces inside
            const string regexString    = "(?<=^[^']*(?:'[^']*'[^']*)*)[\n\r ](?=(?:[^']*'[^']*')*[^']*$)";
            string[] sqlTokenArray      = Regex.Split(model.SqlQuery, regexString).Where(x => !String.IsNullOrEmpty(x)).ToArray();

            // Get the table name
            TableName = FindTableName(sqlTokenArray);
            model.TableName = TableName;
            FindColumns(sqlTokenArray);

            // Build the SELECT statement with Columns
            sb.Append("SELECT ");
            for (int i = 0; i < Columns.Count; ++i)
            {
                sb.Append(Columns[i]).Append(i + 1 != Columns.Count ? ", " : "");
            }
            sb.Append(" FROM ").Append(TableName);

            // No WHERE clause immediatly exit!
            if (IndexOfWhere == -1)
                goto exit;
            sb.Append(" WHERE ");

            // Build up the rest of the query
            BuildQueryAfterWhereClause(sqlTokenArray, sb);

            // Modify the SQL query to remove the contraints we will have applied against documentDB
            var myList          = sqlTokenArray.ToList();
            int IndexOfGroup    = Array.FindIndex(sqlTokenArray, x => x.Equals("GROUP", StringComparison.InvariantCultureIgnoreCase));
            int IndexOfBy       = Array.FindIndex(sqlTokenArray, x => x.Equals("BY", StringComparison.InvariantCultureIgnoreCase));
            if (IndexOfBy != -1 && IndexOfGroup != -1 && (IndexOfGroup + 1 == IndexOfBy))
            {
                myList.RemoveRange(IndexOfWhere, IndexOfGroup - IndexOfWhere);
            }
            else
            {
                myList.RemoveRange(IndexOfWhere, myList.Count - IndexOfWhere);
            }

            StringBuilder sb1 = new StringBuilder();// newSqlQuery = "";
            foreach (string s in myList)
                sb1.Append(s).Append(" ");
            model.SqlQuery = sb1.ToString();

exit:
            string docDBQuery = sb.ToString();
            docDBQuery = ReplaceColumnsWithAlias(docDBQuery);
            return docDBQuery;
        }
    }
}
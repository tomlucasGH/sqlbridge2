using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Text.RegularExpressions;

namespace SqlTokenizerNameSpace
{
    public class SqlParsedInfo
    {
        public string OperationName { get; set; } // Example: SELECT
        public List<string> ColumnsInSelectClause { get; set; }
        public string TableName { get; set; }
        public string WhereClause { get; set; }

        // Default constructor
        public SqlParsedInfo()
        {
            OperationName = "SELECT"; // Assume it is always a SELECT
            ColumnsInSelectClause = new List<string>(); // Maybe change to string
            WhereClause = "WHERE ";
        }
    }

    class SqlTokenizer
    {
        // private variables
        private TSqlTokenType[] ValueTokenTypes = new[]
        {
            TSqlTokenType.Identifier,
            TSqlTokenType.QuotedIdentifier,
            TSqlTokenType.Integer,
            TSqlTokenType.AsciiStringLiteral,
            TSqlTokenType.AsciiStringOrQuotedIdentifier
        };
        private TSqlTokenType[] ComparisonOperators = new[]
        {
            TSqlTokenType.EqualsSign,
            TSqlTokenType.GreaterThan,
            TSqlTokenType.LessThan,
            TSqlTokenType.Bang,
            //TSqlTokenType.In,
            //TSqlTokenType.Not,
            TSqlTokenType.Like,
            TSqlTokenType.Between
        };
        private TSqlTokenType[] ContinuationTokenTypes = new[]
        {
            TSqlTokenType.Or,
            TSqlTokenType.And
        };
        private SqlParsedInfo SqlParsedInfoObject;

        // Default constructor
        public SqlTokenizer()
        {
            SqlParsedInfoObject = new SqlParsedInfo();
        }

        private int FindNextTokenType(IList<TSqlParserToken> tokens, int index, TSqlTokenType tokenType)
        {
            while (index < tokens.Count && tokens[index].TokenType != tokenType)
            {
                ++index;
            }

            // We didnt find the token
            if (index == tokens.Count)
                index = -1;

            return index;
        }

        private int FindNextTokenGivenTokenTypeArray(IList<TSqlParserToken> tokens, int index, TSqlTokenType[] tokenTypeArray)
        {
            while (index < tokens.Count && !tokenTypeArray.Contains(tokens[index].TokenType))
            {
                ++index;
            }

            // We didnt find the token
            if (index == tokens.Count)
                index = -1;

            return index;
        }
        
        // LIKE-->CONTAINS,=, !=, <, >, <=, >=, <> (can't remember what that one does...)
        private int FindNextComparisonToken(IList<TSqlParserToken> tokens, int index, ref string returnedString)
        {
            StringBuilder sb = new StringBuilder("");
            // Find first token if it is there
            int indexOfToken1 = FindNextTokenGivenTokenTypeArray(tokens, index, ComparisonOperators);
            if (indexOfToken1 == -1)
            {
                return -1;
            }

            // Special case the LIKE token, we shouldn't search for more compare operators
            if (tokens[indexOfToken1].TokenType == TSqlTokenType.Like)
            {
                returnedString = "CONTAINS";
                return indexOfToken1 + 1;
            }

            // Append the token and continue searching!
            sb.Append(tokens[indexOfToken1].Text);

            // Find second token if it is there
            int indexOfToken2 = FindNextTokenGivenTokenTypeArray(tokens, indexOfToken1 + 1, ComparisonOperators);
            if (indexOfToken2 == -1 || indexOfToken2 != (indexOfToken1 + 1))
            {
                returnedString = sb.ToString();
                return indexOfToken1 + 1;
            }
            sb.Append(tokens[indexOfToken2].Text);
            // TODO: Here we can validate that we actually got a real comparison operator
            //       Example: I think "!!" or "==" is invalid here.
            returnedString = sb.ToString();
            return indexOfToken2 + 1;
        }

        private int FindNextValueToken(IList<TSqlParserToken> tokens, int index)
        {
            return FindNextTokenGivenTokenTypeArray(tokens, index, ValueTokenTypes);
        }

        // AND, OR
        private int FindNextContinuationToken(IList<TSqlParserToken> tokens, int index)
        {
            return FindNextTokenGivenTokenTypeArray(tokens, index, ContinuationTokenTypes);
        }

        // Could be: TableName.Identifier OR Identifier
        private int FindNextIdentifier(IList<TSqlParserToken> tokens, int index, ref string identifier)
        {
            index = FindNextTokenType(tokens, index, TSqlTokenType.Identifier);
            if (index == -1)
                return -1;

            StringBuilder sb = new StringBuilder("");
            if (index + 2 < tokens.Count && tokens[index + 1].TokenType == TSqlTokenType.Dot)
            {
                // We found the table name
                if (SqlParsedInfoObject.TableName == null)
                    SqlParsedInfoObject.TableName = tokens[index].Text;

                // tableName.Identifier
                sb.Append(tokens[index].Text).Append(".").Append(tokens[index + 2].Text);
                index += 2;
            }
            else
            {
                // Identifier
                if (SqlParsedInfoObject.TableName != null)
                {
                    sb.Append(SqlParsedInfoObject.TableName).Append(".").Append(tokens[index].Text);
                }
                else
                {
                    sb.Append(tokens[index].Text);
                }
            }

            identifier = sb.ToString();
            return index + 1;
        }

        private int HandleCountStatement(IList<TSqlParserToken> tokens, int index)
        {
            while (index < tokens.Count && tokens[index].TokenType != TSqlTokenType.RightParenthesis)
            {
                ++index;
            }
            
            return (index == tokens.Count) ? -1 : index + 1;
        }

        // (1) First find all the columns
        // Return index after we get to "FROM"
        private int FindColumns(IList<TSqlParserToken> tokens, int index)
        {
            // Keep adding identifiers until we hit "FROM"
            while(tokens[index].TokenType != TSqlTokenType.From)
            {
                // Skip whitespace
                if (tokens[index].TokenType == TSqlTokenType.WhiteSpace)
                {
                    ++index;
                    continue;
                }

                // We are selecting all the columns, exit immediately
                if (tokens[index].TokenType == TSqlTokenType.Star)
                {
                    return index + 1;
                }

                // Handle COUNT(...), basically ignore it, increment the index
                if (tokens[index].Text.ToUpper().Equals("COUNT"))
                {
                    index = HandleCountStatement(tokens, index);
                    if (index == -1)
                        return -1;
                    continue;
                }

                string identifier = "";
                index = FindNextIdentifier(tokens, index, ref identifier);
                if (index == -1)
                    return -1;

                SqlParsedInfoObject.ColumnsInSelectClause.Add(identifier);
                ++index;
            }
            return index + 1;
        }

        // (2) This is called after we find the columns
        private int FindTableName(IList<TSqlParserToken> tokens, int index)
        {
            index = FindNextTokenType(tokens, index, TSqlTokenType.Identifier);
            SqlParsedInfoObject.TableName = tokens[index].Text;

            // Fix all the columns that don't have TableName. in front of the Identifier
            for (int i = 0; i < SqlParsedInfoObject.ColumnsInSelectClause.Count; ++i)
            {
                if (!SqlParsedInfoObject.ColumnsInSelectClause[i].Contains("."))
                {
                    SqlParsedInfoObject.ColumnsInSelectClause[i] = SqlParsedInfoObject.TableName + "." + SqlParsedInfoObject.ColumnsInSelectClause[i];
                }
            }

            return index + 1;
        }

        private int HandleBetweenOperator(IList<TSqlParserToken> tokens, int index, StringBuilder sb)
        {
            int limit = index + 5;
            // We already added the between keyword just add the rest!
            if (index + 5 < tokens.Count)
            {
                while (index <= limit)
                {
                    sb.Append(tokens[index].Text);
                    ++index;
                }
            }
            else
            {
                return -1;
            }

            return (index>=tokens.Count) ? -1 :index + 1;
        }
        // (3) Find Constraints on WHERE clause
        private int FindContraintsAfterWhere(IList<TSqlParserToken> tokens, int index)
        {
            StringBuilder sb = new StringBuilder();
            index = FindNextTokenType(tokens, index, TSqlTokenType.Where);
            if (index == -1)
                return -1;

            ++index; // We found WHERE, now skip it to find all the constraints!
            // TODO: Fix later for nested SELECT, but assume WHERE is the last thing we will see
            while (index != -1 && index < tokens.Count)
            {
                // Find the Identifier
                //index = FindNextTokenType(tokens, index, TSqlTokenType.Identifier);
                string identifier = "";
                index = FindNextIdentifier(tokens, index, ref identifier);
                if (index == -1)
                    continue;
                sb.Append("(").Append(identifier).Append(" ");
                //sb.Append(SqlParsedInfoObject.TableName).Append(".").Append(tokens[index].Text);

                // Find the Comparison Operator
                string comparisonOperator = "";
                index = FindNextComparisonToken(tokens, index, ref comparisonOperator);
                if (index == -1)
                    continue;
                sb.Append(comparisonOperator).Append(" ");

                // Continue searching for the next expression
                if (comparisonOperator.Equals("BETWEEN"))
                {
                    // index - 1 = BETWEEN, index = ' ', index + 1 = "Some new identifier!"
                    index = HandleBetweenOperator(tokens, index + 1, sb);
                    if (index == -1)
                    {
                        sb.Append(")");
                        continue;
                    }
                }
                else
                {
                    // Find the Value After Comparison Operator
                    index = FindNextValueToken(tokens, index);
                    if (index == -1)
                        continue;
                    sb.Append(tokens[index].Text);

                    string value = tokens[index].Text;
                    // Special case CONTAINS - THIS ONLY WORKS IF LIKE STATEMENT HAS "%" on beginning and end
                    // For Example: where applicationname like '%VLDataServices%'
                    if (comparisonOperator.Equals("CONTAINS") &&
                        value.StartsWith("'%") &&
                        value.EndsWith("%'"))
                    {
                        value = value.Replace("%", "");
                        sb.Clear();
                        sb.Append("CONTAINS(").Append(identifier).Append(", ").Append(value);//.Append(") ");
                    }
                }

                // Find the Next AND/OR operator
                sb.Append(") ");
                index = FindNextContinuationToken(tokens, index);
                if (index == -1)
                    continue;
                sb.Append(" ").Append(tokens[index].Text).Append(" ");
            }
            SqlParsedInfoObject.WhereClause += sb.ToString();
            return (index == -1) ? -1 : index + 1;
        }

        public SqlParsedInfo BradFunction(string sqlQuery)
        {
            // Remove spaces between strings/words, and remove trailing/ending spaces
            sqlQuery = sqlQuery.Trim();
            Regex regex = new Regex(@"[ ]{2,}"); // Remove spaces greater than two
            sqlQuery = regex.Replace(sqlQuery, @" ");

            var                 sqlParser       = new TSql120Parser(true);
            TextReader          tReader         = new StringReader(sqlQuery.Trim());
            IList<ParseError>   sqlParseErrors;
            int                 index           = 0;
            IList<TSqlParserToken> tokens;

            // Loop through all the tokens
            tokens  = sqlParser.GetTokenStream(tReader, out sqlParseErrors);
            index   = FindColumns(tokens, index + 1); // Assume we will skip the SELECT statement
            index   = FindTableName(tokens, index);
            index   = FindContraintsAfterWhere(tokens, index);

            return SqlParsedInfoObject;
        }

    } // class
} // namespace

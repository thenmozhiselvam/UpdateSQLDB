
using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;

namespace UpdateSQL
{
    public class Program
    {
        private static log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        static string Filepath = string.Empty;
        static string userName = string.Empty;
        static string password = string.Empty;

        static void Main(string[] args)
        {
            try
            {
                log4net.GlobalContext.Properties[Constants.R_OBJECT_ID] = string.Empty;
                if (ConfigurationManager.AppSettings[Constants.SQL_TYPE].ToString().ToUpper() == Constants.ONPREMISESSQL.ToUpper())
                {
                    ReadFile();
                }
                else if (ConfigurationManager.AppSettings[Constants.SQL_TYPE].ToString().ToUpper() == Constants.AZURESQL.ToUpper())
                {
                    Console.WriteLine("Enter the username to connect Azure SQL db :");
                    userName = Console.ReadLine();
                    //Console.WriteLine("Enter the password :");
                    Readpassword();

                    if (!string.IsNullOrEmpty(userName) && !string.IsNullOrEmpty(password))
                        ReadFile();
                    else
                        Console.WriteLine("Please enter proper username and password");
                }
            }
            catch (Exception ex)
            {
                log.Error($"Exception occurred in Main method :{ex.Message} ,Details: {ex.InnerException}");
            }
            Console.ReadLine();
        }

        public static void ReadFile()
        {
            try
            {
                log.Info("In ReadFile method");
                SQLDBModify sQLDBModify = new SQLDBModify();
                Console.WriteLine("Enter the File path (.csv) for fetching LibraryData :");
                Filepath = Console.ReadLine();
                if (!string.IsNullOrEmpty(Filepath))
                {
                    if (Path.GetExtension(Filepath).ToUpper() == ".CSV")
                    {
                        DataTable csvData = ConvertCSVtoDataTable(Filepath);
                        log.Info("Number of rows in CSV file : " + csvData.Rows.Count);
                        Console.WriteLine("File read successful.");
                        Console.WriteLine("Processing data......");
                        sQLDBModify.Update(csvData, userName, password);
                        Console.WriteLine("Processing data successful");
                        Console.WriteLine("Generated log file in given configured path");
                    }
                    else
                    {
                        log.Info("Invalid file extension");
                        Console.WriteLine("Invalid file extension");
                        ReadFile();
                    }
                }
                else
                {
                    log.Info("Invalid File path to read");
                    Console.WriteLine("Invalid File path to read");
                    ReadFile();
                }
            }
            catch (Exception ex)
            {
                log.Info("Error : " + ex.Message);
                Console.WriteLine("Error :" + ex.Message);
            }
        }

        public static DataTable ConvertCSVtoDataTable(string strFilePath)
        {
            DataTable csvdatarows = new DataTable();
            try
            {

                using (StreamReader streamReader = new StreamReader(strFilePath))
                {
                    string[] headers = streamReader.ReadLine().Split(',');
                    foreach (string header in headers)
                    {
                        csvdatarows.Columns.Add(header);
                    }
                    while (!streamReader.EndOfStream)
                    {
                        string[] rows = streamReader.ReadLine().Split(',');
                        DataRow dr = csvdatarows.NewRow();
                        for (int i = 0; i < headers.Length; i++)
                        {
                            dr[i] = rows[i];
                        }
                        csvdatarows.Rows.Add(dr);
                    }

                }
            }
            catch (Exception ex)
            {
                log.Error($"Exception occurred in ConvertCSVtoDataTable method :{ex.Message} ,Details: {ex.InnerException}");
            }

            return csvdatarows;
        }

        public static string Readpassword()
        {
            Console.WriteLine("Enter the password :");
            ConsoleKeyInfo info = Console.ReadKey(true);
            while (info.Key != ConsoleKey.Enter)
            {
                if (info.Key != ConsoleKey.Backspace)
                {
                    Console.Write("*");
                    password += info.KeyChar;
                }
                else if (info.Key == ConsoleKey.Backspace)
                {
                    if (!string.IsNullOrEmpty(password))
                    {
                        password = password.Substring(0, password.Length - 1);
                        int pos = Console.CursorLeft;
                        Console.SetCursorPosition(pos - 1, Console.CursorTop);
                        Console.Write(" ");
                        Console.SetCursorPosition(pos - 1, Console.CursorTop);
                    }
                }
                info = Console.ReadKey(true);
            }
            Console.WriteLine();
            return password;
        }
    }

}

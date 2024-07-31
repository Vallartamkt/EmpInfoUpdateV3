using System;
using System.IO;
using System.Collections;
using System.Collections.Generic;
//using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
//using System.Drawing;
//using System.Linq;
using System.Text;
//using System.Threading.Tasks;
using Microsoft.SqlServer.Server;
using Microsoft.VisualBasic.FileIO;
using System.Text.RegularExpressions;
using WinSCP;
using System.Runtime.ExceptionServices;

namespace EmpInfoUpdateV3
{
    public partial class MainClass
    {
        static void Main()
        {
            sp_LoadEmpInfo();
        }
        static void sp_LoadEmpInfo()
        {
            int[] colIdx = new int[1];
            string[] fldname = new string[1];
            string[] prmname = new string[1];
            decimal[] fldamt = new decimal[1];

            int x = -1;
            x = SetNames(x, "Other_Hours", ref fldname, ref prmname);
            x = SetNames(x, "Other_Wages", ref fldname, ref prmname);
            x = SetNames(x, "Calc_Hourly", ref fldname, ref prmname);
            x = SetNames(x, "RegWages", ref fldname, ref prmname);
            x = SetNames(x, "Reg_Hours", ref fldname, ref prmname);
            x = SetNames(x, "Overtime_Wages", ref fldname, ref prmname);
            x = SetNames(x, "Overtime_Hours", ref fldname, ref prmname);
            x = SetNames(x, "Holiday_Wages", ref fldname, ref prmname);
            x = SetNames(x, "Holiday_Hours", ref fldname, ref prmname);
            x = SetNames(x, "PTO_Hours", ref fldname, ref prmname);
            x = SetNames(x, "PTO_Wages", ref fldname, ref prmname);
            x = SetNames(x, "MPP_Wages", ref fldname, ref prmname);
            x = SetNames(x, "Sick_Hours", ref fldname, ref prmname);
            x = SetNames(x, "Sick_Wages", ref fldname, ref prmname);
            x = SetNames(x, "Vacation_Hours", ref fldname, ref prmname);
            x = SetNames(x, "Vacation_Wages", ref fldname, ref prmname);
            x = SetNames(x, "LA_Predictability_Pay", ref fldname, ref prmname);
            x = SetNames(x, "LA_Predictability_Pay_Hours", ref fldname, ref prmname);
            x = SetNames(x, "MPP_Hours", ref fldname, ref prmname);
            x = SetNames(x, "Double_Time_Hours", ref fldname, ref prmname);
            x = SetNames(x, "Double_Time_Wages", ref fldname, ref prmname);
            x = SetNames(x, "Gross_Hours", ref fldname, ref prmname);
            x = SetNames(x, "True_Working_Wages", ref fldname, ref prmname);
            x = SetNames(x, "True_Working_Hours", ref fldname, ref prmname);
            x = SetNames(x, "BPP_Wages", ref fldname, ref prmname);
            x = SetNames(x, "BPP_Hours", ref fldname, ref prmname);
            x = SetNames(x, "Split_Shift_Payment_Wages", ref fldname, ref prmname);
            x = SetNames(x, "Split_Shift_Payment_Hours", ref fldname, ref prmname);
            Array.Resize(ref fldamt, fldname.Length);

            SqlConnectionStringBuilder gmcsb = new SqlConnectionStringBuilder();
            gmcsb.IntegratedSecurity = true;
            gmcsb.DataSource = "gmcsql01";
            gmcsb.InitialCatalog = "Paycom";
            SqlConnection gmccon = new SqlConnection(gmcsb.ConnectionString);
            SqlConnection gmcInsCon = new SqlConnection(gmcsb.ConnectionString);

            SqlCommand command = new SqlCommand("UpdatePayroll", gmcInsCon);
            command.CommandType = CommandType.StoredProcedure;

            foreach (string file in Directory.GetFiles("c:\\temp\\PayrollUpdate"))
            {
                string Filename = "C:\\temp\\PayrollUpdate\\" + file;
                using (TextFieldParser parser = new TextFieldParser(@file))
                {
                    parser.TextFieldType = FieldType.Delimited;
                    parser.SetDelimiters(",");
                    string[] headerRdr = parser.ReadFields();

                    Array.Resize(ref colIdx, headerRdr.Length);
                    for (int i = 0; i! < headerRdr.Length; i++)
                    {
                        colIdx[i] = 999;
                    }

                    for (int i = 0; i! < headerRdr.Length; i++)
                    {
                        if (Array.Exists(fldname, element => element.StartsWith(headerRdr[i])))
                        {
                            colIdx[i] = Array.IndexOf(fldname, headerRdr[i]);
                        }
                        else if (i > 15)
                        {
                            if (headerRdr[i].IndexOf("Hour") > 0)
                            {
                                colIdx[i] = 0;
                            }
                            else
                            {
                                colIdx[i] = 1;
                            }

                        }
                    }
                    while (!parser.EndOfData)
                    {

                        string[] rdr = parser.ReadFields();

                        gmcInsCon.Open();
                        command.Parameters.Clear();
                        command.Parameters.Add("@Paycom_code", SqlDbType.NChar, 10).Value = rdr[0];
                        command.Parameters.Add("@Transaction_Number", SqlDbType.NChar, 10).Value = rdr[2];
                        command.Parameters.Add("@Check_Number", SqlDbType.Int).Value = int.Parse(rdr[3]);
                        command.Parameters.Add("@PayDate", SqlDbType.Date).Value = DateTime.Parse(rdr[4]);
                        command.Parameters.Add("@Adjust", SqlDbType.NChar, 10).Value = rdr[5];
                        command.Parameters.Add("@Period_End_Date", SqlDbType.Date).Value = DateTime.Parse(rdr[6]);
                        command.Parameters.Add("@Department_Idx", SqlDbType.Int).Value = int.Parse(rdr[7]);
                        command.Parameters.Add("@Payroll_Profile_Code", SqlDbType.NChar, 10).Value = rdr[8];
                        command.Parameters.Add("@Store_Code", SqlDbType.NChar, 3).Value = rdr[9];
                        command.Parameters.Add("@District_Code", SqlDbType.NChar, 3).Value = rdr[10];
                        command.Parameters.Add("@Job_Title_Idx", SqlDbType.Int).Value = int.Parse(rdr[11]);
                        command.Parameters.Add("@Paycheck_Distribution_code", SqlDbType.NChar, 3).Value = rdr[12];
                        command.Parameters.Add("@PayType", SqlDbType.NChar, 3).Value = rdr[13];
                        command.Parameters.Add("@Employee_Code", SqlDbType.NChar, 4).Value = rdr[14];
                        command.Parameters.Add("@Employee_Name", SqlDbType.NChar, 50).Value = rdr[15];
                        for (int i = 0; i! < fldamt.Length; i++)
                        {
                            fldamt[i] = 0;
                        }
                        for (int i = 0; i! < colIdx.Length; i++)
                        {
                            if (colIdx[i] != 999)
                            {
                                fldamt[colIdx[i]] = fldamt[colIdx[i]] + Decimal.Parse(rdr[i]);
                            }
                        }
                        for (int i = 0; i! < fldname.Length; i++)
                        {
                            command.Parameters.Add(prmname[i], SqlDbType.Decimal).Value = fldamt[i];

                        }

                        Console.WriteLine(rdr[2] + " - " + rdr[3]);
                        command.ExecuteNonQuery();

                        gmcInsCon.Close();
                    }
                    gmccon.Close();


                    parser.Close();
                    System.IO.File.Delete(@file);
                }

            }
        }

        static bool GetFilesFromSftpSite(string InputFileName, string LocalPath)
        {
            try
            {
                //Setup session options
                SessionOptions sessionOpts = new SessionOptions
                {
                    Protocol = Protocol.Sftp,
                    //HostName = AppSettings["SFTPHost"],
                    //UserName = AppSettings["SFTPUsername"],
                    //Password = AppSettings["SFTPPassword"],
                    //SshHostKeyFingerprint = AppSettings["SFTPFingerPrint"]
                };

                using (var sess = new Session())
                {
                    // Connect
                    sess.Open(sessionOpts);
                    string remotePath = "/Home/000_0rf73_sftp/outbound/";
                    LocalPath += "\\";
                    TransferOperationResult tor = sess.GetFilesToDirectory(remotePath, LocalPath, "*_" + InputFileName + "_*.csv", true);
                    if (tor.IsSuccess)
                    {
                        return true;
                    }
                }
                return false;
            }
            catch (Exception e)
            {

                //Program.MailMsg = Program.MailMsg + "\n" + "Error: {0} " + e.Message + " in GetFilesFromSftpSite - " + DateTime.Now;
                Console.WriteLine("Error: {0}", e);
                return false;
            }

        }
        static int SetNames(int i, string name, ref string[] fldNames, ref string[] prmNames)
        {
            i++;
            Array.Resize(ref fldNames, i + 1);
            Array.Resize(ref prmNames, i + 1);
            fldNames[i] = name;
            prmNames[i] = "@" + name;
            return i;
        }
    }
}




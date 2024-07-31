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
            string[] fldName = new string[1];
            string[] prmName = new string[1];
            int[] fldTyp = new int[1];
            string[] exempt = { "SS_Number", "Zipcode","ClockSeq" };

            SqlConnectionStringBuilder gmcsb = new SqlConnectionStringBuilder();
            gmcsb.IntegratedSecurity = true;
            gmcsb.DataSource = "gmcsql01";
            gmcsb.InitialCatalog = "Paycom";
            SqlConnection gmccon = new SqlConnection(gmcsb.ConnectionString);
            SqlConnection gmcInsCon = new SqlConnection(gmcsb.ConnectionString);

            SqlCommand command = new SqlCommand("UpdateEmpInfo", gmcInsCon);
            command.CommandType = CommandType.StoredProcedure;

            foreach (string file in Directory.GetFiles("c:\\temp\\EmpInfoUpdate"))
            {
                string Filename = "C:\\temp\\EmpInfoUpdate\\" + file;
                using (TextFieldParser parser = new TextFieldParser(@file))
                {
                    parser.TextFieldType = FieldType.Delimited;
                    parser.SetDelimiters(",");
                    string[] headerRdr = parser.ReadFields();

                    Array.Resize(ref colIdx, headerRdr.Length - 1);
                    int f = -1;

                    for (int i = 0; i != headerRdr.Length; i++)
                    {
                        f = SetNames(f, headerRdr[i], ref fldName, ref prmName);
                    }
                    Array.Resize(ref fldTyp, f+1);

                    string[] rdr = parser.ReadFields();
                    int tmpOut = 0;
                    for (int i = 0; i! < fldTyp.Length; i++)
                    {
                        if (headerRdr[i].Contains("Date"))
                        {
                            fldTyp[i] = 3;
                        }
                        else if (int.TryParse(rdr[i], out tmpOut))
                        {
                            if (Array.Exists(exempt, element => element.StartsWith(headerRdr[i])))
                            {
                                fldTyp[i] = 1;
                            }
                            else
                            {
                                fldTyp[i] = 2;
                            }
                        }
                        else
                        {
                            fldTyp[i] = 1;
                        }
                    }
                    while (!parser.EndOfData)
                    {

                        gmcInsCon.Open();
                        command.Parameters.Clear();
                        for (int x=0; x !< fldName.Length; x++)
                        {
                            switch (fldTyp[x])
                            {
                                case > 2:
                                    command.Parameters.Add(prmName[x], SqlDbType.Date).Value = DateTime.Parse(rdr[x]);
                                    break;
                                case > 1:
                                    if (rdr[x] == "")
                                    {
                                        command.Parameters.Add(prmName[x], SqlDbType.Int).Value = 0;
                                    }
                                    else
                                    {
                                        command.Parameters.Add(prmName[x], SqlDbType.Int).Value = int.Parse(rdr[x]);
                                    }
                                    break;
                                default:
                                    command.Parameters.Add(prmName[x], SqlDbType.VarChar).Value = rdr[x];
                                    break;
                            }
                                
                        }
                      
                        Console.WriteLine(rdr[2] + " - " + rdr[3]);
                        command.ExecuteNonQuery();

                        gmcInsCon.Close();
                        rdr = parser.ReadFields();
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




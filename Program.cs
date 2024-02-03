using MiniExcelLibs;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FileMatcher
{
    internal class Program
    {

        static DirectoryInfo origin_dir;
        static DirectoryInfo target_dir;

        static string root_dir = AppDomain.CurrentDomain.BaseDirectory;

        static DataTable dt = new DataTable();

        static Dictionary<string, string> o_map = new Dictionary<string, string>();
        static Dictionary<string, string> t_map = new Dictionary<string, string>();

        static string o_excel_path,t_excel_path,result_excel_path;

        static void Main(string[] args)
        {

            Console.WriteLine("HaoJun0823 FileMatcher https://blog.haojun0823.xyz/");
            if (args.Length < 2)
            {

                Console.WriteLine("You Need 2 Args: Origin Directonary Target Directory");


            }
            else
            {

                if (args.Length >= 3)
                {
                    root_dir = args[2];
                }

                o_excel_path = root_dir + "\\FM_origin.xlsx";
                t_excel_path = root_dir + "\\FM_target.xlsx";
                result_excel_path = root_dir + "\\FM_result.xlsx";

                Console.WriteLine($"Execel Will Be Save to {o_excel_path},{t_excel_path},{result_excel_path}");

                origin_dir = new DirectoryInfo(args[0]);
                target_dir = new DirectoryInfo(args[1]);

                if (!origin_dir.Exists) {

                    origin_dir.Create();

                }

                if (!target_dir.Exists)
                {

                    target_dir.Create();
                }


                Console.WriteLine($"Origin:{origin_dir.FullName}");
                Console.WriteLine($"Target:{target_dir.FullName}");


                dt.Columns.Add("origin_path", typeof(string));
                dt.Columns.Add("origin_md5", typeof(string));
                dt.Columns.Add("target_path", typeof(string));
                dt.Columns.Add("target_md5", typeof(string));


                var o_files = origin_dir.GetFiles("*.*", SearchOption.AllDirectories);
                var t_files = target_dir.GetFiles("*.*", SearchOption.AllDirectories);

                Console.WriteLine($"Origin Folder File Number:{o_files.Length}");
                Console.WriteLine($"Target Folder File Number:{t_files.Length}");


                int o_count = 0, t_count = 0;

                Parallel.Invoke(() => {

                    Parallel.ForEach<FileInfo>(o_files, i =>
                    {
                        string md5 = GetMD5(File.ReadAllBytes(i.FullName));
                        o_map.Add(i.FullName, md5);
                        Console.WriteLine($"[Origin][{++o_count}/{o_files.Length}]Add {i.FullName},MD5:{md5}");




                    });




                }, () => {


                    Parallel.ForEach<FileInfo>(t_files, i =>
                    {
                        string md5 = GetMD5(File.ReadAllBytes(i.FullName));
                        t_map.Add(i.FullName, md5);

                        Console.WriteLine($"[Target][{++t_count}/{t_files.Length}]Add {i.FullName},MD5:{md5}");
                    });


                });

                var t_map_list = t_map.ToList<KeyValuePair<string, string>>();

                int o2_count = 0;

                Parallel.ForEach<KeyValuePair<string, string>>(o_map, kv =>
                {

                    
                    Console.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}]({++o2_count}/{o_map.Count})Search {kv.Key}({kv.Value})");


                    if (t_map.ContainsValue(kv.Value)){

                        Console.WriteLine($"{kv.Key}({kv.Value}) Match Target Directory Files");

                        var query = from pair in t_map_list where pair.Value == kv.Value select pair;

                        Parallel.ForEach<KeyValuePair<string, string>>(query, kv2 =>
                        {

                            lock (dt) { 

                            DataRow dr = dt.NewRow();

                            dr["origin_path"] = kv.Key;
                            dr["origin_md5"] = kv.Value;

                            dr["target_path"] = kv2.Key;
                            dr["target_md5"] = kv2.Value;

                            dt.Rows.Add(dr);
                            }
                            Console.WriteLine($"Add Row:{kv.Key}<=>{kv2.Key}");

                        });


                        //foreach(var kv2 in query)
                        //{

                        //    lock (dt) { 

                        //    DataRow dr = dt.NewRow();

                        //    dr["origin_path"] = kv.Key;
                        //    dr["origin_md5"] = kv.Value;

                        //    dr["target_path"] = kv2.Key;
                        //    dr["target_md5"] = kv2.Value;

                        //    dt.Rows.Add(dr);
                        //    }
                        //    Console.WriteLine($"Add Row:{kv.Key}<=>{kv2.Key}");
                        //}
                        



                    }
                    else
                    {
                        Console.WriteLine($"{kv.Key}({kv.Value}) Not Match Target Directory Files");
                    }



                });




                SaveExcel();

            }

            Console.WriteLine("Press Any Key To Exit...");
            Console.ReadKey();

        }


        static string GetMD5(byte[] data)
        {

            MD5 md5 = new MD5CryptoServiceProvider();


            var md5_bytes = md5.ComputeHash(data);

            StringBuilder sb = new StringBuilder();

            foreach(var i in md5_bytes)
            {
                sb.Append(i.ToString("X"));
            }

            return sb.ToString();


        }

        static void SaveExcel()
        {


            Parallel.Invoke(() =>
            {
                if (File.Exists(result_excel_path))
                {
                    File.Delete(result_excel_path);
                }
                using (FileStream fs = File.OpenWrite(result_excel_path))
                {
                    MiniExcel.SaveAs(fs, dt);

                }
            },
            () =>
            {
                if (File.Exists(t_excel_path))
                {
                    File.Delete(t_excel_path);
                }
                using (FileStream fs = File.OpenWrite(t_excel_path))
                {
                    MiniExcel.SaveAs(fs, GetDataTable(t_map));
                }
            },
            () =>
            {
                if (File.Exists(o_excel_path))
                {
                    File.Delete(o_excel_path);

                    using (FileStream fs = File.OpenWrite(o_excel_path))
                    {
                        MiniExcel.SaveAs(fs, GetDataTable(o_map));
                    }
                }
            });



        }

        static DataTable GetDataTable(Dictionary<string,string> map)
        {

            DataTable dt = new DataTable();

            dt.Columns.Add("KEY",typeof(string));
            dt.Columns.Add("VALUE", typeof(string));

            foreach (var kv in map)
            {

                DataRow dr = dt.NewRow();

                dr["KEY"] = kv.Key;
                dr["VALUE"] = kv.Value;

                dt.Rows.Add(dr);


            }

            return dt;
        }

    }
}

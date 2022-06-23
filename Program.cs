using System;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;

namespace influxdb_eval
{
    class Program
    {
        static async System.Threading.Tasks.Task Main(string[] args)
        {
            var influxDBClient = InfluxDBClientFactory.Create("http://localhost:8086", "rtx9K2a0yKPF3sNsMabdx4mE2J-lR9Rugo4PidWBHUuryFbH3VpurI96fa965I0GYEpdj1wLt_LCjz83PdscOQ==");
            influxDBClient.SetLogLevel(InfluxDB.Client.Core.LogLevel.None);

            //var writeOptions = WriteOptions
            //.CreateNew()
            //.BatchSize(500)
            //.FlushInterval(10)
            //.Build();

            var writeOptions = WriteOptions
            .CreateNew()
            .BatchSize(50000)
            .FlushInterval(1000)
            .Build();

            var writeApi = influxDBClient.GetWriteApi(writeOptions);
            var orgname = (await influxDBClient.GetOrganizationsApi().FindOrganizationByIdAsync("81494d02eabbc9e4")).Name;
            
            var bucketApi = influxDBClient.GetBucketsApi();
            var bucketApiOrg = (await bucketApi.FindBucketsByOrgNameAsync(orgname));

            var buck = bucketApiOrg.Find(a => a.OrgID == "81494d02eabbc9e4" && a.Name == "messung");

            if (buck != null)
            {
                try
                {
                    bucketApi.DeleteBucketAsync(buck).RunSynchronously();
                } catch {

                }
            }

            var retention = new System.Collections.Generic.List<BucketRetentionRules>(){new BucketRetentionRules(BucketRetentionRules.TypeEnum.Expire, 36000)};
            await bucketApi.CreateBucketAsync(new Bucket(name:"messung", retentionRules:retention, orgID:"81494d02eabbc9e4"));

            int vars = 100;
            int length = 1000;
            int per = 1000;
            int intervall = 10;
            int k = 0;
            
            var ran = new System.Random();

            // Start the child process.
            var p = new System.Diagnostics.Process();
            // Redirect the output stream of the child process.
            p.StartInfo.UseShellExecute = false;
            p.StartInfo.RedirectStandardOutput = true;
            p.StartInfo.FileName = "/usr/bin/du";
            p.StartInfo.Arguments = "-s";
            p.StartInfo.WorkingDirectory = "/home/dbb/.influxdbv2/engine";
            

            for (int i = 0; i < length; ++i)
            {
                var points = new System.Collections.Generic.List<InfluxDB.Client.Writes.PointData>();

                for (int j = 0; j < vars; ++j)
                {
                    var point = InfluxDB.Client.Writes.PointData.Measurement("var_" + j.ToString())
                    .Tag("plant", "1")
                    .Field("value", ran.NextDouble());
                    points.Add(point);
                }

                if (k == intervall)
                {
                    p.Start();
                    // Do not wait for the child process to exit before
                    // reading to the end of its redirected stream.
                    // p.WaitForExit();
                    // Read the output stream first and then wait.
                    string output = p.StandardOutput.ReadToEnd();
                    p.WaitForExit();
                    try
                    {
                        var point = InfluxDB.Client.Writes.PointData.Measurement("influxdb_size")
                        .Tag("plant", "1")
                        .Field("value", Int64.Parse(output.Split('\t')[0]));
                        points.Add(point);
                        k = 0;                        
                    } catch {

                    }
                } else {
                    ++k;
                }

                writeApi.WritePoints(points, "messung", "81494d02eabbc9e4");
                System.Threading.Thread.Sleep(per);
            }

            influxDBClient.Dispose();

            return;
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Hadoop;
using Microsoft.Hadoop.MapReduce;
using Microsoft.Hadoop.WebClient.WebHCatClient;
using RaleighCrime;

namespace RaleighCrimeConsole
{
    class Program
    {
        static void Main(string[] args)
        {
			/* To create these locations on your own drive, open the Hadoop 
			 console and then type in the following commands:
				hadoop fs -mkdir /user/OpenData
				hadoop fs -mkdir /user/OpenData/Police
				hadoop fs -copyFromLocal C:\Temp\Datasets\Police.csv /user/OpenData/Police/
				hadoop fs -mkdir /user/OpenData/Output
			 */
            HadoopJobConfiguration config = new HadoopJobConfiguration();
            config.InputPath = "/user/OpenData/Police";
            config.OutputFolder = "/user/OpenData/Output";

			//Replace the URI with your local machine name.
			//Note that password is ignored for the HDInsight emulator, so that can be whatever you want.
            Uri clusterURI = new Uri("http://yourmachine");
            string username = "hadoop";
	        string password = null;
            IHadoop cluster = Hadoop.Connect(clusterURI, username, password);

            Console.WriteLine("Crime Counter.  Select an option to continue:");
            Console.WriteLine("1) Raw count by crime");
            Console.WriteLine("2) Count by coordinates (4 spots after decimal)");

            var input = Console.ReadLine();

            MapReduceResult jobResult;
            switch (input)
            {
                case "1":
                    jobResult = cluster.MapReduceJob.Execute<CrimeCount, TotalCrimeCount>(config);
                    break;
                case "2":
                    //Quick note:  if we just wanted to spit out all areas regardless of 
                    //number of crimes, we could just use the TotalCrimeCount class
                    //and would not need to generate a new Reduce class.
                    jobResult = cluster.MapReduceJob.Execute<CrimeLocation, TopCrimeLocations>(config);
                    break;
                default:
                    return;
            }

            int exitcode = jobResult.Info.ExitCode;
            string exitstatus = exitcode == 0 ? "Success" : "Failure";

            Console.WriteLine();
            Console.WriteLine("Exit Code = " + exitstatus);
            Console.Read();
        }
    }
}

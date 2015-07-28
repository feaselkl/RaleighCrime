// Learn more about F# at http://fsharp.net
// See the 'F# Tutorial' project for more help.
open System
open Microsoft.Hadoop
open Microsoft.Hadoop.MapReduce
open Microsoft.Hadoop.WebClient.WebHCatClient
open System.Text.RegularExpressions
//open RaleighCrime

(* Data set:  https://data.raleighnc.gov/Police/Police-Incident-Data-All-Dates-Master-File/csw9-dd5k
 * This contains Raleigh crime data from 2005 through 2013.
 * 
 * Our data set looks like the following:
 * LCR = Crime Code
 * LCR DESC = Crime decription
 * INC DATETIME = incident date and time
 * BEAT = officer at incident
 * INC NO = incident number
 * LOCATION = coordinates; good to 14 spots after decimal
 * 
 * Sample rows:
 * LCR,LCR DESC,INC DATETIME,BEAT,INC NO,LOCATION
 * 750,MISC/FOUND PROPERTY,04/16/2014 02:37:00 AM,211,P14049666,"(35.81972932157367, -78.62496454367128)"
 * 269,"ALL OTHER/ALL OTHER OFFENSES (COMM.THREATS, ETC.)",04/15/2014 12:58:00 PM,431,P14049404,"(35.747619351764705, -78.6196101713265)"
 * 
 * The data set is comma-separated, with quotation
 * marks around values containing commas.
 *)

//We're looking to get a raw count, so I just want to use "1" here and sum up all the 1s later.
[<Literal>]
let value = "1"
//This regular expression splits out our comma-separated string 
//and handles commas within quotation marks as part of the value.
//For example:  1,2,"3,4,5" would break out into three groups:  1, 2, and 3,4,5
let commaSplit = new Regex("(?:^|,)(\"(?:[^\"]+|\"\")*\"|[^,]*)", RegexOptions.Compiled);

//Crime Count
type CrimeCount() =
    inherit MapperBase()
    override x.Map(inputLine, context) =
        let matches = commaSplit.Matches(inputLine)
        match matches.Count with
                | 6 -> let key = (matches |> Seq.cast |> Seq.nth 1).ToString().TrimStart(',').Trim('\"')
                       match key with
                             | "LCR DESC" -> 0 |> ignore
                             | k -> context.EmitKeyValue(k, value)
                | i -> 0 |> ignore
        0 |> ignore

type TotalCrimeCount() =
    inherit ReducerCombinerBase()
    override x.Reduce(key, values, context) =
        let numberOfCrimes = values |> Seq.sumBy(fun (v) -> Int32.Parse(v))
        context.EmitKeyValue(key, numberOfCrimes.ToString())

//Crime Location
type CrimeLocation() =
    inherit MapperBase()
    override x.Map(inputLine, context) =
        let matches = commaSplit.Matches(inputLine)
        match matches.Count with
                | 6 ->  let coords = (matches |> Seq.cast |> Seq.nth 5).ToString().TrimStart(',').Trim('\"').Trim('(').Trim(')').Split(',')
                        match coords.Length with
                                | 2 ->  let lat, long = coords.[0].Trim(' ').Substring(0, coords.[0].IndexOf('.') + 4), coords.[1].Trim(' ').Substring(0, coords.[1].IndexOf('.') + 3)
                                        context.EmitKeyValue(lat + "," + long, value)
                                | n -> 0 |> ignore
                | i -> 0 |> ignore


type TopCrimeLocations() =
    inherit ReducerCombinerBase()
    override x.Reduce(key, values, context) =
        //Get the sum of values per key. This will return our final counts.
        let numberOfCrimes = values |> Seq.sumBy(fun (v) -> Int32.Parse(v))
        if (numberOfCrimes >= 400)
            then context.EmitKeyValue(key, numberOfCrimes.ToString())

[<EntryPoint>]
let main argv = 
    (* To create these locations on your own drive, open the Hadoop 
	    console and then type in the following commands:
	    hadoop fs -mkdir /user/OpenData
	    hadoop fs -mkdir /user/OpenData/Police
	    hadoop fs -copyFromLocal C:\Temp\Datasets\Police.csv /user/OpenData/Police/
	    hadoop fs -mkdir /user/OpenData/Output
	*)
    let config = new HadoopJobConfiguration()
    config.InputPath <- "/user/OpenData/Police"
    config.OutputFolder <- "/user/OpenData/Output"

    //Replace the URI with your local machine name.
    //Note that password is ignored for the HDInsight emulator, so that can be whatever you want.
    let clusterURI = new Uri("http://STEYN:50070")
    let cluster = Hadoop.Connect(clusterURI, "hadoop", null);

    printfn "F# Crime Counter.  Select an option to continue:"
    printfn "1) Raw count by crime"
    printfn "2) Count by coordinates (4 spots after decimal)"

    let input = Console.ReadLine()
    match input with
        | "1" -> let jobResult = cluster.MapReduceJob.Execute<CrimeCount, TotalCrimeCount>(config)
                 printfn "Exit Code:  %A" jobResult.Info.ExitCode
        | "2" -> let jobResult = cluster.MapReduceJob.Execute<CrimeLocation, TopCrimeLocations>(config)
                 printfn "Exit Code:  %A" jobResult.Info.ExitCode
        | x -> printfn "%A is an invalid input" x

    printfn "%A" argv
    let ok = Console.ReadLine()
    0 // return an integer exit code

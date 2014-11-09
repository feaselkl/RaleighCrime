using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Hadoop.MapReduce;
using System.Text.RegularExpressions;

namespace RaleighCrime
{
    //Our goal is to use MapReduce to find out how many
    //times each crime was committed.  For this first exmaple,
    //we just want a simple count.
    public class CrimeCount : MapperBase
    {
		/* Data set:  https://data.raleighnc.gov/Police/Police-Incident-Data-All-Dates-Master-File/csw9-dd5k
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
		 */

		//We're looking to get a raw count, so I just want to use "1" here and sum up all the 1s later.
		private const string value = "1";

        //This regular expression splits out our comma-separated string 
        //and handles commas within quotation marks as part of the value.
        //For example:  1,2,"3,4,5" would break out into three groups:  1, 2, and 3,4,5
        private Regex commaSplit = new Regex("(?:^|,)(\"(?:[^\"]+|\"\")*\"|[^,]*)", RegexOptions.Compiled);

        public override void Map(string inputLine, MapperContext context)
        {
            var matches = commaSplit.Matches(inputLine);

			//Hadoop is semi-structured, meaning that we do not need every row to match
			//our expected structure.  This is a simplistic check, but given a more polyglot
			//data set, we could filter out unexpected rows or perform more complex filtering.
            if (matches == null || matches.Count != 6)
            {
				//If this were a production data set, I would think about logging bad rows
				//but because this is a demo, I just want to return without passing anything
				//to be reduced.
                return;
            }

            //Trim off any leading comma or surrounding quotation marks.
            string key = matches[1].Value.TrimStart(',').Trim('\"');

            //We don't want to return the header row.
            if (key == "LCR DESC")
            {
                return;
            }

            //The Map function returns a set of key-value pairs.
            //The key in this case is the specific crime, and our value
            //is "1" (which we will use to get a count).
            context.EmitKeyValue(key, value);
        }
    }

    public class TotalCrimeCount : ReducerCombinerBase
    {
        public override void Reduce(string key, IEnumerable<string> values, ReducerCombinerContext context)
        {
            //Get the sum of values per key.  This will return our final counts.
            context.EmitKeyValue(key, values.Sum(e => long.Parse(e)).ToString());
        }
    }
}

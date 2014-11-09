using System;
using Microsoft.Hadoop.MapReduce;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text.RegularExpressions;

namespace RaleighCrimeTests
{
    [TestClass]
    public class UnitTest1
    {
	    [TestMethod]
        public void RegexParser_TestHeader()
        {
            Regex commaSplit = new Regex("(?:^|,)(\"(?:[^\"]+|\"\")*\"|[^,]*)", RegexOptions.Compiled);
            string TestString = "LCR,LCR DESC,INC DATETIME,BEAT,INC NO,LOCATION";

            var matches = commaSplit.Matches(TestString);
            Assert.AreEqual(6, matches.Count);
            Assert.AreEqual("LCR DESC", matches[1].Value.TrimStart(',').Trim('\"'));
        }

        [TestMethod]
        public void RegexParser_TestDataLineWithQuotes()
        {
            Regex commaSplit = new Regex("(?:^|,)(\"(?:[^\"]+|\"\")*\"|[^,]*)", RegexOptions.Compiled);
            string TestString = "119,FRAUD/ALL OTHER,04/15/2014 06:32:00 PM,433,P14049504,\"(35.75334263487902, -78.5533945258969)\"";

            var matches = commaSplit.Matches(TestString);
            Assert.AreEqual(6, matches.Count);
            Assert.AreEqual("FRAUD/ALL OTHER", matches[1].Value.TrimStart(',').Trim('\"'));
            Assert.AreEqual("(35.75334263487902, -78.5533945258969)", matches[5].Value.TrimStart(',').Trim('\"'));
        }

        [TestMethod]
        public void RegexParser_TestDataLineWithoutQuotes()
        {
            Regex commaSplit = new Regex("(?:^|,)(\"(?:[^\"]+|\"\")*\"|[^,]*)", RegexOptions.Compiled);
            string TestString = "119,FRAUD/ALL OTHER,04/15/2014 06:32:00 PM,433,P14049504,(35.75334263487902 -78.5533945258969)";

            var matches = commaSplit.Matches(TestString);
            Assert.AreEqual(6, matches.Count);
            Assert.AreEqual("FRAUD/ALL OTHER", matches[1].Value.TrimStart(',').Trim('\"'));
            Assert.AreEqual("(35.75334263487902 -78.5533945258969)", matches[5].Value.TrimStart(',').Trim('\"'));
        }

        [TestMethod]
        public void RegexParser_TestDataLineWithQuoteInCrime()
        {
            Regex commaSplit = new Regex("(?:^|,)(\"(?:[^\"]+|\"\")*\"|[^,]*)", RegexOptions.Compiled);
            string TestString = "269,\"ALL OTHER/ALL OTHER OFFENSES (COMM.THREATS, ETC.)\",04/15/2014 10:33:00 AM,424,P14049395,\"(35.775769267765874, -78.61143830117872)\"";

            var matches = commaSplit.Matches(TestString);
            Assert.AreEqual(6, matches.Count);
            Assert.AreEqual("ALL OTHER/ALL OTHER OFFENSES (COMM.THREATS, ETC.)", matches[1].Value.TrimStart(',').Trim('\"'));
            Assert.AreEqual("(35.775769267765874, -78.61143830117872)", matches[5].Value.TrimStart(',').Trim('\"'));
        }

        [TestMethod]
        public void CoordinateSplit_GetFourDecimalPoints()
        {
            string expectedResults = "35.7762, -78.6246";
            string input = "(35.776238687249744, -78.6246378053371)";

            string key = input.TrimStart(',').Trim('\"').Trim('(').Trim(')');
            var keyvals = key.Split(',');

            key = keyvals[0].Trim(' ').Substring(0, keyvals[0].IndexOf('.') + 5)
                  + ", " + keyvals[1].Trim(' ').Substring(0, keyvals[1].IndexOf('.') + 4);

            Assert.AreEqual(expectedResults, key);
        }
    }
}

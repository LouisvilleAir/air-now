using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
//using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Fclp;

namespace APCD.AirNow
{
    class GetSouthernIndianaData
    {
        static void Main(string[] args)
        {

            #region Set up parameters.
            String airNowFile = String.Empty;
            String errorMessage = String.Empty;
            String baseURI = String.Empty;
            String downloadFolder = String.Empty;
            String finalFile = String.Empty;
            String finalFolder = String.Empty;
            int hoursToGet = 4; // hours to (re)retrieve by default
            int hoursToLookBack = 120; // default maximum hours to search back
            bool isOK = false;
            bool isVerbose = false;

            var p = new FluentCommandLineParser();

            p.Setup<int>('h', "hoursback")
                .WithDescription("The number of hours to force (re)download. If the program has NOT run successfully for previous hours, it will download data up to 120 hours back or the specified hours back, whichever is larger.")
                .Callback(hours => hoursToGet = hours)
                .SetDefault(4);

            p.Setup<string>('u', "baseuri")
                .WithDescription("The website and folder to download AirNow files from")
                .Callback(uri => baseURI = uri)
                .SetDefault(@"https://s3-us-west-1.amazonaws.com/files.airnowtech.org/airnow/today/");

            p.Setup<string>('d', "downloadfolder")
                .WithDescription("Where to download AirNow hourly data files to")
                .Callback(folder => downloadFolder = folder)
                .SetDefault(@"C:\Temp\AirNow\");

            p.Setup<string>('i', "idemfolder")
                .Callback(folder => finalFolder = folder)
                .WithDescription("Where to save the Southern Indiana EST data files")
                .SetDefault(@"C:\Temp\AirVisionImport\");

            p.Setup<bool>('v', "verbose")
                .Callback(verbose => isVerbose = verbose)
                .WithDescription("Show messages while the program is running?")
                .SetDefault(false);

            p.Parse(args);
            #endregion

            #region Data validation
            // Check if the hours back is valid.
            if (hoursToGet > 0)
                isOK = true;
            else
            {
                errorMessage += "Hours to get must be a positive whole number; " + hoursToGet.ToString() + " is not valid. ";
                isOK = false;
            }
            // Check if the base URI is valid.
            Uri uriResult;

            if (Uri.TryCreate(baseURI, UriKind.Absolute, out uriResult)
                && (uriResult.Scheme == Uri.UriSchemeHttp || uriResult.Scheme == Uri.UriSchemeHttps))
            {
                baseURI = uriResult.AbsoluteUri;
                isOK = true;
            }
            else
            {
                errorMessage += "Invalid Web address '" + baseURI + "'. ";
                isOK = false;
            }

            // Check if the download folder for AirNow files exists.
            if (Directory.Exists(downloadFolder))
            {
                // Make sure the folder path ends with a backslash.
                if (!downloadFolder.EndsWith(@"\"))
                    downloadFolder += @"\";

                isOK = true;
            }
            else
            {
                errorMessage += "Folder does not exist: '" + downloadFolder + "'. ";
                isOK = false;
            }

            // Check if the destination folder for IDEM EST files exists.
            if (Directory.Exists(finalFolder))
            {
                if (!finalFolder.EndsWith(@"\"))
                    finalFolder += @"\";

                isOK = true;
            }
            else
            {
                errorMessage += "Folder does not exist: '" + finalFolder + "'. ";
                isOK = false;
            }
            #endregion

            if (isOK)
            {
                // Check for existing raw files.
                try
                {
                    if (hoursToGet > 1)
                        hoursToGet = Math.Max(HoursSinceLastDownload(hoursToLookBack, downloadFolder), hoursToGet);
                    else
                        hoursToGet = HoursSinceLastDownload(hoursToLookBack, downloadFolder);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("\nError: " + ex.Message);
                    isOK = false;
                }

                int hoursLatest = 1;
                // The latest data file becomes available about 30 minutes after the hour.
                if (DateTime.Now.Minute < 30)
                    hoursLatest = 2;

                for (int i = hoursToGet; i >= hoursLatest; i--)
                {
                    // Get and process new raw files.
                    airNowFile = String.Empty;
                    try
                    {
                        airNowFile = DownloadHourlyDataFile(i, baseURI, downloadFolder);
                        if (isVerbose)
                        {
                            Console.WriteLine("\nSuccesfully downloaded File \"{0}\" from \"{1}\"", airNowFile, baseURI);
                            Console.WriteLine("Downloaded file saved as:\n\t" + airNowFile);
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("\nError: " + ex.Message);
                        isOK = false;
                    }

                    if (airNowFile.Length > 0)
                    {
                        finalFile = String.Empty;
                        try
                        {
                            finalFile = ParseOutSouthernIndiana(airNowFile, finalFolder);
                            if (isVerbose)
                            {
                                if (finalFile.Length > 0)
                                    Console.WriteLine("\nIDEM EST file saved as:\n\t" + finalFile);
                                else
                                    Console.WriteLine("\nNo IDEM EST output file produced!");
                            }
                            //File.Delete(rawFile);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("\nError: " + ex.Message);
                            isOK = false;
                        }
                        finally
                        {
                            System.Threading.Thread.Sleep(100);
                        }
                    }
                }

                // For debugging:
                //Console.WriteLine("\nPress a key to continue...");
                //Console.ReadKey();
            }
            else
            {
                if (errorMessage.Length > 0)
                {
                    Console.WriteLine("\nError:" + errorMessage);
                }
            }
        }

        static String DownloadHourlyDataFile(int hoursAgo, String baseURI, String localPath)
        {
            DateTime timeUTC = System.DateTime.UtcNow.AddHours(-hoursAgo);
            String filename = "HourlyData_" + timeUTC.ToString("yyyyMMddHH") + ".dat";
            String fullURL = baseURI + filename;
            String localFile = localPath + filename;

#if DEBUG
            Console.WriteLine("\nFilename: " + filename);
#endif
            
            // Create a new WebClient instance.
            WebClient client = new WebClient();

            try
            {
                client.Headers.Add("user-agent", "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko");
                // Download the Web resource and save it into the current filesystem folder.
                client.DownloadFile(fullURL, localFile);
            }
            catch(WebException ex)
            {
                if (ex.Status == WebExceptionStatus.ProtocolError &&
                  ex.Response != null)
                {
                    HttpWebResponse resp = (HttpWebResponse)ex.Response;
                    if (resp.StatusCode != HttpStatusCode.NotFound)
                        throw;
                }
                else
                    throw;
            }
            catch(Exception)
            {
                throw;
            }
            finally
            {
                client.Dispose();
                // For debugging:
                //Console.WriteLine("\nPress a key to continue...");
                //Console.ReadKey();
            }
            return localFile;
        }

        static String ParseOutSouthernIndiana(String inFile, String outPath)
        {
            const char _delimiter = '|';
            bool isOK = true;
            String lineDateFormat = "MM'/'dd'/'yy'" + _delimiter + "'HH:mm";
            String filenameDateFormat = "yyyyMMddHH";
            String outFile = String.Empty;
            int i = inFile.IndexOf("HourlyData_") + 11;
            DateTime timeUTC = DateTime.ParseExact(inFile.Substring(i, 10), filenameDateFormat, CultureInfo.InvariantCulture);
            if (timeUTC > DateTime.MinValue)
            {
                DateTime timeEST = timeUTC.AddHours(-5);
                String newTimeString = timeEST.ToString(lineDateFormat);
                outFile = outPath + "IDEM_EST_" + timeEST.ToString(filenameDateFormat) + ".dat";

                // Delete the file if it already exists.
                try
                {
                    if (File.Exists(outFile))
                        File.Delete(outFile);
                }
                catch (Exception)
                {         
                    throw;
                }

                if (isOK)
                {
                    try
                    {
                        StreamReader reader = File.OpenText(inFile);
                        StreamWriter writer = new StreamWriter(outFile);
                        string line;
                        while ((line = reader.ReadLine()) != null)
                        {
                            string[] items = line.Split(_delimiter);
                            String test = items[2].Substring(0, 5); // FIPS state + county
                            if (test == "18019" || test == "18043") // Clark or Floyd County, Indiana
                            {
                                // For debugging:
                                //Console.WriteLine("\nOriginal line:\n" + line);

                                String newLine = newTimeString + line.Substring(14);

                                writer.WriteLine(newLine);
                                // For debugging:
                                //Console.WriteLine("\n\nNew line:\n" + newLine);
                            }
                        }
                        reader.Close();
                        writer.Close();
                    }
                    catch (Exception)
                    {
                        throw;
                    }
                    //finally
                    //{
                    
                    //}
                }
            }
            else
            {
                Console.WriteLine("\nError parsing date and time from filename \"" + inFile + "\"!");
                outFile = String.Empty;
            }

            return outFile;
        }

        static int HoursSinceLastDownload(int hoursToLookBack, String finalFolder)
        {
            int hoursResult = hoursToLookBack;
            int startHours = 0; // hours back from now

            // See if the computer is on Daylight Savings Time.
            if (TimeZone.CurrentTimeZone.IsDaylightSavingTime(DateTime.Now))
            {
                startHours = 1;
            }
            for (int i = startHours; i < hoursToLookBack; i++)
            {
                DateTime testUTCTime = DateTime.UtcNow.AddHours(-i); // i hours ago UTC
                String testFile = finalFolder + "HourlyData_" + testUTCTime.ToString("yyyyMMddHH") + ".dat";
                try
                {
                    if (File.Exists(testFile))
                    {
                        hoursResult = i;
                        break;
                    }
                }
                catch (Exception)
                {
                    throw;
                }
            }

            return hoursResult;
        }
    }
}

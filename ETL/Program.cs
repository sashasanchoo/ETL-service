using System;
using System.Configuration;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace ETL
{
    public class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("ETL running...");
            DirectoriesManager directoriesManager = DirectoriesManager.GetInstance();
            directoriesManager.CreateDailyDirectory();
            LogFileManager logManager = LogFileManager.GetInstance();
            RequiredForValidationOperationsRunner runner = new RequiredForValidationOperationsRunner();
            ExitRequestWaitingRunner exitRequestWaitingRunner = new ExitRequestWaitingRunner();
            MidnightResetRunner midnightWorker = new MidnightResetRunner();
            DataToJsonConverter concreteDataToJsonConverter = new DataToJsonConverter();
            CancellationTokenHolder cancellationTokenHolder = CancellationTokenHolder.GetInstance();
            try
            {
                Task.Run(() =>
                {
                   exitRequestWaitingRunner.WaitForExitRequest(cancellationTokenHolder.GetCancellationTokenSource());
                }, cancellationTokenHolder.GetToken());
                Task.Run(() =>
                {
                    midnightWorker.CheckIfMidnight(cancellationTokenHolder.GetCancellationTokenSource(), directoriesManager.CreateDailyDirectory, directoriesManager.GetDailyDirectoryInfo(), logManager.CreateLogFile, logManager.Reset); ;
                }, cancellationTokenHolder.GetToken());
                do
                {
                    try
                    {
                        runner.ValidFilesCollection = runner.DirValidator.CheckConcreteDirectory(directoriesManager.GetFolderAPath(), logManager.GetInvalidFilesList());
                        if (runner.ValidFilesCollection.Count() > 0)
                        {
                            runner.BeginValidationProcess(concreteDataToJsonConverter.ConvertValidDataToJsonObjects, directoriesManager.GetDailyDirectoryInfo().FullName, logManager.IncreaseFoundErrors, concreteDataToJsonConverter.ValidDataCollection);
                        }
                    }
                    catch (Exception)
                    {}
                   
                } while (ExitRequestWaitingRunner.ExitRequested == false);
            }
            catch (Exception)
            {}
            finally
            {
                logManager.CreateLogFile(directoriesManager.GetDailyDirectoryInfo());
            }
        }
    }
    //AncillaryItems
    #region
    public class MidnightResetRunner
    {
        private const string midnight = "23:59:59";
        public void CheckIfMidnight(CancellationTokenSource cts, Action createDailyDirectory, DirectoryInfo dailyDirectoryInfo, Action<DirectoryInfo> createLogFile, Action reset)
        {
            while (true)
            {
                try
                {
                    if (DateTime.Now.ToLongTimeString() == midnight)
                    {
                        createLogFile(dailyDirectoryInfo);
                        reset.Invoke();
                        Thread.Sleep(1020);
                        createDailyDirectory.Invoke();
                    }
                    cts.Token.ThrowIfCancellationRequested();
                }
                catch (OperationCanceledException)
                {
                    ExitRequestWaitingRunner.ExitRequested = true;
                }

            }

        }
    }
    public sealed class CancellationTokenHolder
    {
        private static CancellationTokenHolder instance;

        private static CancellationTokenSource cancellationTokenSource;

        private static CancellationToken token;
        private CancellationTokenHolder() { }

        public static CancellationTokenHolder GetInstance()
        {
            if (instance == null)
            {
                instance = new CancellationTokenHolder();
                cancellationTokenSource = new CancellationTokenSource();
                token = cancellationTokenSource.Token;
            }
            return instance;
        }
        public CancellationToken GetToken()
        {
            return token;
        }
        public CancellationTokenSource GetCancellationTokenSource()
        {
            return cancellationTokenSource;
        }

    }
    public class ExitRequestWaitingRunner
    {
        public static bool ExitRequested { get; set; }
        public void WaitForExitRequest(CancellationTokenSource cts)
        {
            while (true)
            {
                try
                {
                    string answer = Console.ReadLine();
                    switch (answer)
                    {
                        case "exit":
                            {
                                cts.Cancel();
                                break;
                            }
                        default:
                            {
                                Console.Clear();
                                Console.WriteLine("Wrong command");
                                break;
                            }
                    }
                    cts.Token.ThrowIfCancellationRequested();
                }
                catch (OperationCanceledException)
                {
                    ExitRequested = true;
                    return;
                }

            }
        }
    }
    public sealed class LogFileManager
    {
        private static LogFileManager instance;

        private static string logFileName;
        private static int foundErrors;
        private static List<string> invalidFilesList;
        private LogFileManager() { }

        public static LogFileManager GetInstance()
        {
            if (instance == null)
            {
                instance = new LogFileManager();
                logFileName = "meta.log";
                invalidFilesList = new List<string>();
            }
            return instance;
        }
        public void CreateLogFile(DirectoryInfo dailyDirectory)
        {
            File.AppendAllText(@$"{dailyDirectory.FullName}\{logFileName}", $"found_errors: {foundErrors}\n");
            File.AppendAllText(@$"{dailyDirectory.FullName}\{logFileName}", $"invalid files:\n{string.Join(" ", string.Join("\n", invalidFilesList))}");
        }
        public void Reset()
        {
            foundErrors = 0;
            invalidFilesList.Clear();
        }
        public void IncreaseFoundErrors()
        {
            foundErrors++;
        }
        public List<string> GetInvalidFilesList()
        {
            return invalidFilesList;
        }
    }
    public class RequiredForValidationOperationsRunner
    {
        public DirectoryValidator DirValidator { get; private set; }
        private ValidFilesManager ValidFilesManager { get; set; }
        private ValidFileDataReader ValidFileDataReader { get; set; }
        public IEnumerable<FileSystemInfo> ValidFilesCollection { get; set; }

        public RequiredForValidationOperationsRunner()
        {
            DirValidator = new DirectoryValidator();
            ValidFileDataReader = new ValidFileDataReader();
            ValidFilesManager = new ValidFilesManager();
            ValidFilesCollection = new List<FileSystemInfo>();

        }
        public void BeginValidationProcess(Func<StringBuilder> customOperationWithValidData, string targetDirectoryPath, Action increaseErrors, ICollection<string> validDataCollection)
        {
            foreach (var validFile in ValidFilesCollection)
            {
                ValidFileDataValidator.CheckDataFromConcreteValidFile(ValidFileDataReader.ReadFromConcreteValidFile(validFile.FullName), validDataCollection, Patterns.MainPattern, increaseErrors);
                ValidFilesManager.StartWriteValidFiles(validFile, customOperationWithValidData.Invoke(), targetDirectoryPath);
            }
        }
    }
    public class StreamDecorator : IDisposable
    {
        private Stream Stream { get; set; }
        private string Data { get; set; }
        private byte[] ReadBuff { get; set; }
        private byte[] WriteBuff { get; set; }

        public StreamDecorator(Stream stream)
        {
            this.Stream = stream;
        }
        public void Dispose()
        {
            Stream?.Dispose();
        }
        public string Read()
        {
            ReadBuff = new byte[Stream.Length];
            Stream.ReadAsync(ReadBuff, 0, ReadBuff.Length).Wait();
            Data = Encoding.UTF8.GetString(ReadBuff, 0, ReadBuff.Length);
            return Data;
        }
        public bool Write(string data)
        {
            WriteBuff = Encoding.UTF8.GetBytes(data);
            Stream.WriteAsync(WriteBuff, 0, WriteBuff.Length).Wait();;
            return true;
        }
    }
    #endregion
    //CustomBusinessModel
    #region
    public interface IPaymentBuilder
    {
        void BuildCity(string city);
        void BuildServices(Service service);
        void BuildTotal(long total);
        void Reset();
        Payment GetPayment();
    }
    public interface IServicesBuilder
    {
        void BuildName(string name);
        void BuildPayer(Payer payer);
        void BuildTotal(long total);
        void Reset();
        Service GetService();
    }
    public interface IPayerBuilder
    {
        void BuildName(string name);
        void BuildPayment(decimal payment);
        void BuildDate(DateTime date);
        void BuildAccountNumber(long accountNumber);
        void Reset();
        Payer GetPayer();

    }
    public class PaymentBuilder : IPaymentBuilder
    {
        private Payment payment = new Payment();
        public PaymentBuilder()
        {
            Reset();
        }
        public void Reset()
        {
            payment = new Payment();
        }
        public void BuildCity(string city)
        {
            payment.City = city;
        }

        public void BuildServices(Service service)
        {
            payment.Services = service;
        }

        public void BuildTotal(long total)
        {
            payment.Total = total;
        }
        public Payment GetPayment()
        {
            Payment result = payment;
            Reset();
            return result;
        }
    }
    public class ServicesBuilder : IServicesBuilder
    {
        private Service service = new Service();
        public ServicesBuilder()
        {
            Reset();
        }
        public void Reset()
        {
            service = new Service();
        }
        public void BuildName(string name)
        {
            service.Name = name;
        }

        public void BuildPayer(Payer payer)
        {
            service.Payers = payer;
        }

        public void BuildTotal(long total)
        {
            service.Total = total;
        }
        public Service GetService()
        {
            Service result = service;
            Reset();
            return result;
        }
    }

    public class PayerBuilder : IPayerBuilder
    {
        private Payer payer = new Payer();
        public PayerBuilder()
        {
            Reset();
        }
        public void Reset()
        {
            payer = new Payer();
        }
        public void BuildAccountNumber(long accountNumber)
        {
            payer.Account_Number = accountNumber;
        }

        public void BuildDate(DateTime date)
        {
            payer.Date = date;
        }

        public void BuildName(string name)
        {
            payer.Name = name;
        }

        public void BuildPayment(decimal payment)
        {
            payer.Payment = payment;
        }
        public Payer GetPayer()
        {
            Payer result = payer;
            Reset();
            return result;
        }
    }
    public class Payment
    {
        public string City { get; set; }
        public Service Services { get; set; }
        public decimal Total { get; set; }
    }
    public class Service
    {
        public string Name { get; set; }
        public Payer Payers { get; set; }
        public decimal Total { get; set; }

    }
    public class Payer
    {
        public string Name { get; set; }
        public decimal Payment { get; set; }
        public DateTime Date { get; set; }
        public long Account_Number { get; set; }
    }
    #endregion
    //Extract
    #region
    public static class FileSystemInfoExtension
    {
        public static bool FileValidation(this FileSystemInfo difInfo)
        {
            if (difInfo.Extension == ".txt")
            {
                return true;
            }
            return false;
        }
    }
    public class DirectoryValidator
    {
        public IEnumerable<FileSystemInfo> CheckConcreteDirectory(string path, List<string> invalidFiles)
        {
            DirectoryInfo validFiles = new DirectoryInfo(path);
            invalidFiles.AddRange(validFiles.GetFileSystemInfos().Where(e => !e.FileValidation() && !invalidFiles.Contains(e.FullName)).Select(f => f.FullName));
            return validFiles.GetFileSystemInfos().Where(e => e.FileValidation());
        }
    }
    public class ValidFileDataValidator
    {
        public static void CheckDataFromConcreteValidFile(string DataToCheck, ICollection<string> validDataHolder, string pattern, Action increaseErrors)
        {
            try
            {
                string[] splitedValidDataHolder = DataToCheck.Split("\n", StringSplitOptions.RemoveEmptyEntries);
                validDataHolder?.Clear();
                foreach (var item in splitedValidDataHolder)
                {
                    if (Regex.Match(item, pattern).Success)
                    {
                        validDataHolder?.Add(item);
                    }
                    else
                    {
                        increaseErrors.Invoke();
                    }
                }
            }
            catch (Exception)
            {
                return;
            }

        }
    }

    public class ValidFileDataReader
    {
        public string ReadFromConcreteValidFile(string path)
        {
            using (StreamDecorator sd = new StreamDecorator(new FileStream(path, FileMode.Open)))
            {
                return sd.Read();
            }
        }
    }
    #endregion
    //Transform
    #region
    static class Patterns
    {
        public static string MainPattern { get; } = @"[a-zA-Z]+\,\s+[a-zA-Z]+\,\s+(“|”)[a-zA-Z0-9\,\s]+(“|”)\,\s+\d+\.\d{1,2}\,\s+\d{4}-\d{2}-\d{2}\,\s(“|”)?[\d]+(“|”)?\,\s+[a-zA-Z]+";
        public static string Adress { get; } = @"“[a-zA-Z0-9\,\s]+”";

        public static string FullName { get; } = @"[a-zA-Z\,\s]+";

        public static string Total { get; } = @"\d+\.\d{1,2}";

        public static string Date { get; } = @"\d{4}-\d{2}-\d{2}";

        public static string Account_Number { get; } = @"\d+";

        public static string ServiceName { get; } = @"[a-zA-Z]+";
    }
    public class ConcretePatternMatchSearcher
    {
        public Match Match { get; private set; }

        public void FindMatch(string ToFindMatch, string pattern)
        {
            Match = Regex.Match(ToFindMatch, pattern);
        }
    }
    public class DataToJsonConverter
    {
        private StringBuilder JsonSerializedObject { get; set; }
        public ICollection<string> ValidDataCollection { get; set; }
        private ConcretePatternMatchSearcher MatchSearcher { get; set; }
        private StringBuilder ValidElements { get; set; }
        private IPaymentBuilder PaymentBuilder { get; set; }

        private IServicesBuilder ServicesBuilder { get; set; }

        private IPayerBuilder PayerBuilder { get; set; }

        public DataToJsonConverter()
        {
            JsonSerializedObject = new StringBuilder();
            ValidDataCollection = new List<string>();
            MatchSearcher = new ConcretePatternMatchSearcher();
            ValidElements = new StringBuilder();
            PaymentBuilder = new PaymentBuilder();
            ServicesBuilder = new ServicesBuilder();
            PayerBuilder = new PayerBuilder();
        }
        public StringBuilder ConvertValidDataToJsonObjects()
        {
            JsonSerializedObject.Clear();
            foreach (var item in ValidDataCollection)
            {
                ValidElements.Clear();
                MatchSearcher.FindMatch(item, Patterns.Adress);
                PaymentBuilder.BuildCity(MatchSearcher.Match.Value);
                ValidElements.Append(item.Replace(MatchSearcher.Match.Value, ""));

                MatchSearcher.FindMatch(ValidElements.ToString(), Patterns.FullName);
                PayerBuilder.BuildName(MatchSearcher.Match.Value.Replace(",", "").TrimEnd()); ;
                ValidElements.Replace(MatchSearcher.Match.Value, "");

                MatchSearcher.FindMatch(ValidElements.ToString(), Patterns.Total);
                PayerBuilder.BuildPayment(Convert.ToInt64(MatchSearcher.Match.Value.Split('.')[0]));
                ValidElements.Replace(MatchSearcher.Match.Value, "");


                MatchSearcher.FindMatch(ValidElements.ToString(), Patterns.Date);
                PayerBuilder.BuildDate(DateTime.ParseExact(MatchSearcher.Match.Value, "yyyy-mm-dd", CultureInfo.CurrentCulture));
                ValidElements.Replace(MatchSearcher.Match.Value, "");

                MatchSearcher.FindMatch(ValidElements.ToString(), Patterns.Account_Number);
                PayerBuilder.BuildAccountNumber(Convert.ToInt64(MatchSearcher.Match.Value));
                ValidElements.Replace(MatchSearcher.Match.Value, "");

                MatchSearcher.FindMatch(ValidElements.ToString(), Patterns.ServiceName);
                ServicesBuilder.BuildName(MatchSearcher.Match.Value);
                ValidElements.Replace(MatchSearcher.Match.Value, "");


                ServicesBuilder.BuildPayer(PayerBuilder.GetPayer());
                PaymentBuilder.BuildServices(ServicesBuilder.GetService());
                JsonSerializedObject.Append(JsonSerializer.Serialize<Payment>(PaymentBuilder.GetPayment(), new JsonSerializerOptions { WriteIndented = true }));
                JsonSerializedObject.Append("\n");
                PayerBuilder.Reset();
                PaymentBuilder.Reset();
                ServicesBuilder.Reset();
            }
            return JsonSerializedObject;
        }
    }
    public class ConvertedFilesDeleter
    {
        public void DeleteConvertedFile(FileSystemInfo fileToMove)
        {
            try
            {
                File.Delete(fileToMove.FullName);
            }
            catch (Exception)
            {
                return;
            }
        }


    }
    #endregion
    //Load
    #region
    public sealed class DirectoriesManager
    {
        private static DirectoriesManager instance;

        private static string folderAPath;
        private static string folderBPath;
        private static DirectoryInfo dailyDirectoryInfo;

        private DirectoriesManager() { }

        public static DirectoriesManager GetInstance()
        {
            if (instance == null)
            {
                instance = new DirectoriesManager();
                folderAPath = ConfigurationManager.AppSettings["pathToFolderA"];
                folderBPath = ConfigurationManager.AppSettings["pathToFolderB"];
            }
            return instance;
        }
        public void CreateDailyDirectory()
        {
            dailyDirectoryInfo = Directory.CreateDirectory($"{folderBPath}{DateTime.Now.ToShortDateString().Replace('.', '-')}");
        }
        public string GetFolderAPath()
        {
            return folderAPath;
        }
        public string GetFolderBPath()
        {
            return folderBPath;
        }
        public DirectoryInfo GetDailyDirectoryInfo()
        {
            return dailyDirectoryInfo;
        }
    }
    public class ValidDataWriter
    {
        private const string fileExtension = "txt";

        private const string fileName = "Output";
        public bool WriteValidData(string data, ref int validFilesCounter, string targetDirectoryPath)
        {
            if (File.Exists(@$"{targetDirectoryPath}{fileName}{++validFilesCounter}.{fileExtension}"))
            {
                return false;
            }
            using (StreamDecorator sd = new StreamDecorator(new FileStream(@$"{targetDirectoryPath}\{fileName}{validFilesCounter}.{fileExtension}", FileMode.CreateNew, FileAccess.Write)))
            {
                return sd.Write(data);
            }
        }
    }
    public class ValidFilesManager
    {
        private ValidDataWriter ValidDataWriter { get; set; }
        private ConvertedFilesDeleter AlreadyConvertedFilesDeleter { get; set; }

        public int ValidFilesCounter;
        public ValidFilesManager()
        {
            ValidDataWriter = new ValidDataWriter();
            AlreadyConvertedFilesDeleter = new ConvertedFilesDeleter();
        }
        public void StartWriteValidFiles(FileSystemInfo tmpFileHolder, StringBuilder dataToWrite, string targetDirectoryPath)
        {
            if (ValidDataWriter.WriteValidData(dataToWrite.ToString(), ref ValidFilesCounter, targetDirectoryPath)) ;
            {
                AlreadyConvertedFilesDeleter.DeleteConvertedFile(tmpFileHolder);
            }
        }
    }
    #endregion Load
}
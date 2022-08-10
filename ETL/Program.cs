using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Configuration;

namespace ETL
{
    public delegate StringBuilder CustomOperationWithValidDataDelegate();

    public class Program
    {
        static void Main(string[] args)
        {
            try
            {
                ValidationManager toValidate = new ValidationManager();
                DirectoriesManager.CreateDailyDirectory();
                CancellationTokenSource cancelTokenSource = new CancellationTokenSource();
                CancellationToken token = cancelTokenSource.Token;
                Task main = new Task(() =>
                {
                    while (true)
                    {
                        toValidate.ValidFilesCollection = toValidate.DirValidator.CheckConcreteDirectory(DirectoriesManager.FolderAPath, LogManager.InvalidFilesList);
                        if (toValidate.ValidFilesCollection.Count() > 0)
                        {
                            toValidate.BeginValidationProcess(DirectoriesManager.FolderAPath, toValidate.CustomOperationWithValidDataDlg);
                        }
                        if (token.IsCancellationRequested)
                        {
                            Console.WriteLine("exit request");
                            return;
                        }
                    }
                }, token);
               
                Task menu = new Task(() =>
                {
                    while (true)
                    {
                        string answer = Console.ReadLine();
                        switch (answer)
                        {
                            case "exit":
                                {
                                    cancelTokenSource.Cancel();
                                    break;
                                }
                        }
                    }
                }, token);
                while (true)
                {
                    main.Start();
                    menu.Start();
                    main.Wait();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }     
        }
    }
    static class DirectoriesManager
    {
        public static DirectoryInfo DailyDirectoryInfo { get; private set; }

        public static string FolderAPath { get; } = ConfigurationManager.AppSettings["pathToFolderA"];
        public static string FolderBPath { get; } = ConfigurationManager.AppSettings["pathToFolderB"];

        public static void CreateDailyDirectory()
        {
            DailyDirectoryInfo = Directory.CreateDirectory($"{FolderBPath}{DateTime.Now.ToShortDateString().Replace('.', '-')}");
        }
    }
    static class LogManager
    {
        public static int FoundErrors { get; set; }
        public static List<string> InvalidFilesList { get; set; } = new List<string>();
        public static void CreateLogFile(DirectoryInfo dailyDirectory)
        {
            File.AppendAllText(@$"{dailyDirectory.FullName}\meta.log", $"found_errors: {FoundErrors}\n");
            File.AppendAllText(@$"{dailyDirectory.FullName}\meta.log", $"invalid files:\n{string.Join(" ", string.Join("\n", InvalidFilesList))}");
        }
    }

    public class ConcretePatternMatchSearcher
    {
        public Match Match { get; private set; }

        public void FindMatch(string ToFindMatch, string pattern)
        {
            Match = Regex.Match(ToFindMatch, pattern);
        }
    }
    class StreamDecorator: IDisposable
    {
        private Stream stream;
        private IAsyncResult result;
        private string data;
        private byte[] readBuff;
        private byte[] writeBuff;
        public StreamDecorator(Stream stream)
        {
            this.stream = stream;
        }
        public void Dispose()
        {
            stream?.Dispose();
        }
        public string Read()
        {
            readBuff = new byte[stream.Length];
            result = stream.BeginRead(readBuff, 0, readBuff.Length, ReadIsComplete, null);
            data = Encoding.UTF8.GetString(readBuff, 0, readBuff.Length);
            return data;
        }
        public void ReadIsComplete(IAsyncResult res)
        {
            Console.WriteLine($"Reading complete. Thread id = {Thread.CurrentThread.ManagedThreadId} started at {DateTime.Now.ToLongTimeString()}");

        }
        public bool Write(string data)
        {
            writeBuff = Encoding.UTF8.GetBytes(data);
            result = stream.BeginWrite(writeBuff, 0, writeBuff.Length, WriteIsComplete, null);   
            return true;
        }
        public void WriteIsComplete(IAsyncResult res)
        {
            Console.WriteLine($"Writing complete. Thread id = {Thread.CurrentThread.ManagedThreadId} started at {DateTime.Now.ToLongTimeString()}");
        }
    }

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
    public class ValidationManager
    {
        public CustomOperationWithValidDataDelegate CustomOperationWithValidDataDlg { get; set; }
        public int CheckedFilesCounter { get; set; }

        public DirectoryValidator DirValidator { get; set; }

        public ValidDataWriter ValidDataWriter { get; set; }

        public CustomJsonConverter ConcreteDataToJsonConverter { get; set; }

        public ValidFilesManager ValidFilesManager { get; set; }
        public ValidFileDataReader ValidFileDataReader { get; set; }
        public IEnumerable<FileSystemInfo> ValidFilesCollection { get; set; }

        public ValidationManager()
        {
            DirValidator = new DirectoryValidator();
            ValidDataWriter = new ValidDataWriter();
            ValidFileDataReader = new ValidFileDataReader();
            ConcreteDataToJsonConverter = new CustomJsonConverter();
            CustomOperationWithValidDataDlg = new CustomOperationWithValidDataDelegate(ConcreteDataToJsonConverter.ConvertValidLinesToJsonObjects);
            ValidFilesManager = new ValidFilesManager();
            ValidFilesCollection = new List<FileSystemInfo>();

        }
        public void BeginValidationProcess(string path, CustomOperationWithValidDataDelegate CustomOperationWithValidData)
        {
            foreach (var validFile in ValidFilesCollection)
            {
                DataValidator.CheckDataFromConcreteValidFile(ValidFileDataReader.ReadFromConcreteValidFile(validFile.FullName), ConcreteDataToJsonConverter.ValidDataList);
                CheckedFilesCounter = ValidFilesManager.StartWriteNewAndRenameOldFiles(validFile, CustomOperationWithValidData.Invoke(), CheckedFilesCounter);
            }
            DirectoriesManager.CreateDailyDirectory();
            LogManager.CreateLogFile(DirectoriesManager.DailyDirectoryInfo);
        }
    }

    public class ValidFilesManager
    {
        public ValidDataWriter ValidDataWriter { get; set; }
        public CheckedFilesMarker AlreadyCheckedFilesMarker { get; set; }
        public ValidFilesManager()
        {
            ValidDataWriter = new ValidDataWriter();
            AlreadyCheckedFilesMarker = new CheckedFilesMarker();
        }
        public int StartWriteNewAndRenameOldFiles(FileSystemInfo tmpFileHolder, StringBuilder dataToWrite, int paramCheckedFilesCounter)
        {
            int checkedFilesCounter = paramCheckedFilesCounter;
            if (ValidDataWriter.WriteValidData(dataToWrite.ToString()))
            {
                AlreadyCheckedFilesMarker.MarkFileAsChecked(tmpFileHolder, ref checkedFilesCounter);
            }
            return checkedFilesCounter;
        }
    }

    public class CustomJsonConverter
    {
        public StringBuilder JsonSerializedObject { get; set; }
        public List<string> ValidDataList { get; set; }
        public ConcretePatternMatchSearcher MatchSearcher { get; set; }
        public StringBuilder ValidElements { get; set; }
        public PaymentBuilder PaymentBuilder { get; set; }

        public ServicesBuilder ServicesBuilder { get; set; }

        public PayerBuilder PayerBuilder { get; set; }

        public CustomJsonConverter()
        {
            JsonSerializedObject = new StringBuilder();
            ValidDataList = new List<string>();
            MatchSearcher = new ConcretePatternMatchSearcher();
            ValidElements = new StringBuilder();
            PaymentBuilder = new PaymentBuilder();
            ServicesBuilder = new ServicesBuilder();
            PayerBuilder = new PayerBuilder();
        }
        public StringBuilder ConvertValidLinesToJsonObjects()
        {
            JsonSerializedObject.Clear();
            foreach (var item in ValidDataList)
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

    public interface IPaymentBuilder
    {
        void BuildCity(string city);
        void BuildServices(Service service);
        void BuildTotal(long total);
    }
    public interface IServicesBuilder
    {
        void BuildName(string name);
        void BuildPayer(Payer payer);
        void BuildTotal(long total);
    }
    public interface IPayerBuilder
    {
        void BuildName(string name);
        void BuildPayment(decimal payment);
        void BuildDate(DateTime date);
        void BuildAccountNumber(long accountNumber);

    }
    public class PaymentBuilder : IPaymentBuilder
    {
        private Payment _payment = new Payment();
        public PaymentBuilder()
        {
            this.Reset();
        }
        public void Reset()
        {
            this._payment = new Payment();
        }
        public void BuildCity(string city)
        {
            this._payment.City = city;
        }

        public void BuildServices(Service service)
        {
            this._payment.Services = service;
        }

        public void BuildTotal(long total)
        {
            this._payment.Total = total;
        }
        public Payment GetPayment()
        {
            Payment result = this._payment;
            this.Reset();
            return result;
        }
    }
    public class ServicesBuilder : IServicesBuilder
    {
        private Service _service = new Service();
        public ServicesBuilder()
        {
            this.Reset();
        }
        public void Reset()
        {
            this._service = new Service();
        }
        public void BuildName(string name)
        {
            this._service.Name = name;
        }

        public void BuildPayer(Payer payer)
        {
            this._service.Payers = payer;
        }

        public void BuildTotal(long total)
        {
            this._service.Total = total;
        }
        public Service GetService()
        {
            Service result = this._service;
            this.Reset();
            return result;
        }
    }

    public class PayerBuilder : IPayerBuilder
    {
        private Payer _payer = new Payer();
        public PayerBuilder()
        {
            this.Reset();
        }
        public void Reset()
        {
            this._payer = new Payer();
        }
        public void BuildAccountNumber(long accountNumber)
        {
            this._payer.Account_Number = accountNumber;
        }

        public void BuildDate(DateTime date)
        {
            this._payer.Date = date;
        }

        public void BuildName(string name)
        {
            this._payer.Name = name;
        }

        public void BuildPayment(decimal payment)
        {
            this._payer.Payment = payment;
        }
        public Payer GetPayer()
        {
            Payer result = this._payer;
            this.Reset();
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

    public static class FileSystemInfoExtension
    {
        public static bool FileValidation(this FileSystemInfo difInfo)
        {
            if((difInfo.Extension == ".txt" || difInfo.Extension == ".csv") && !difInfo.FullName.Contains("Source"))
            {
                return true;
            }
            return false;
        }
    }
   
    public class DataValidator
    {
        public static void CheckDataFromConcreteValidFile(string DataToCheck, ICollection<string> validDataHolder)
        {
            try
            {
                string[] splitedValidDataHolder = DataToCheck.Split("\n", StringSplitOptions.RemoveEmptyEntries);
                validDataHolder?.Clear();
                foreach (var item in splitedValidDataHolder)
                {
                    if (Regex.Match(item, Patterns.MainPattern).Success)
                    {
                        validDataHolder?.Add(item);
                    }
                    else
                    {
                        LogManager.FoundErrors++;
                    }
                }
            }
            catch (Exception)
            {
                return;
            }

        }
    }
    public class DirectoryValidator
    {
        public IEnumerable<FileSystemInfo> CheckConcreteDirectory(string path, List<string> invalidFiles)
        {
            DirectoryInfo validFiles = new DirectoryInfo(path);
            invalidFiles.AddRange(validFiles.GetFileSystemInfos().Where(e => !e.FileValidation()).Select(f => f.FullName));
            return validFiles.GetFileSystemInfos().Where(e => e.FileValidation());
        }
    }
    public class CheckedFilesMarker
    {
        public void MarkFileAsChecked(FileSystemInfo fileToMove, ref int checkedFilesCounter)
        {
            try
            {
                File.Move(fileToMove.FullName, $"{fileToMove.FullName.Replace($"{fileToMove.Name}", $"Source{++checkedFilesCounter}")}{fileToMove.Extension}");
            }
            catch (Exception)
            {
                return;
            }
        }
    }
    public class ValidDataWriter
    {
        public int ValidFilesCounter { get; private set; }

        public bool WriteValidData(string data)
        {
            using (StreamDecorator sd = new StreamDecorator(new FileStream(@$"{DirectoriesManager.DailyDirectoryInfo.FullName}\Output{++ValidFilesCounter}.txt", FileMode.Create, FileAccess.Write, FileShare.None, 0, FileOptions.Asynchronous)))
            {
                return sd.Write(data);
            }
        }
    }
    public class ValidFileDataReader
    {
        public string ReadFromConcreteValidFile(string path)
        {
            using (StreamDecorator sd = new StreamDecorator(new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.None, 0, FileOptions.Asynchronous)))
            {
                return sd.Read();
            }
        }
    }

}

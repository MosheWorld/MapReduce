using MapReduce.Logger;
using MapReduce.FileSystemService;
using MapReduce.DelegateDefenitions;

namespace MapReduce.Reducer
{
    public class Reducer : IReducer
    {
        private readonly ILogger logger;
        private readonly IFileSystemService fileSystemService;
        private readonly ReducerDelegate reduceDelegate;

        public Reducer(ILogger logger, IFileSystemService fileSystemService, ReducerDelegate reduceDelegate)
        {
            this.logger = logger;
            this.fileSystemService = fileSystemService;
            this.reduceDelegate = reduceDelegate;
        }

        public void Reduce(string readDataFilePath, string writeDataDirectoryPath)
        {
            logger.Log($"Started reduce phase, reading data from path:  {readDataFilePath}");

            try
            {
                KeyValuePair<string, string> data = reduceDelegate(logger, readDataFilePath);
                string tempFilePath = Path.Combine(writeDataDirectoryPath, $"{Guid.NewGuid():N}.txt");
                fileSystemService.WriteKeyValuePairToPath(tempFilePath, data);
            }
            catch (Exception ex)
            {
                logger.Error($"Error in reduce: {ex.Message}");
            }

            logger.Log("Finished reduce phase");
        }
    }
}

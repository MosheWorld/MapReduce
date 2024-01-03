using MapReduce.Logger;
using MapReduce.FileSystemService;
using MapReduce.DelegateDefenitions;

namespace MapReduce.Shuffler
{
    public class Shuffler : IShuffler
    {
        private readonly ILogger logger;
        private readonly IFileSystemService fileSystemService;
        private readonly ShuffleDelegate shuffleDelegate;

        public Shuffler(ILogger logger, IFileSystemService fileSystemService, ShuffleDelegate shuffleDelegate)
        {
            this.logger = logger;
            this.fileSystemService = fileSystemService;
            this.shuffleDelegate = shuffleDelegate;
        }

        public void Shuffle(string readDataDirectoryPath, string writeDataDirectoryPath)
        {
            logger.Log($"Started shuffler phase, reading data from directory: ${readDataDirectoryPath}");

            try
            {
                List<string> filePaths = fileSystemService.GetFilePathsFromDirectory(readDataDirectoryPath);
                List<KeyValuePair<string, string>> data = shuffleDelegate(logger, filePaths);

                foreach (KeyValuePair<string, string> entry in data)
                {
                    string filePath = Path.Combine(writeDataDirectoryPath, $"{Guid.NewGuid():N}.txt");
                    fileSystemService.WriteKeyValuePairToPath(filePath, entry);
                }
            }
            catch (Exception ex)
            {
                logger.Error($"Error in shuffler: {ex.Message}");
            }

            logger.Log("Finished shuffler phase");
        }
    }
}

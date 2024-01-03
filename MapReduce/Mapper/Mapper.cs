using MapReduce.Logger;
using MapReduce.FileSystemService;
using MapReduce.DelegateDefenitions;

namespace MapReduce.Mapper
{
    public class Mapper : IMapper
    {
        private readonly ILogger logger;
        private readonly IFileSystemService fileSystemService;
        private readonly MapDelegate mapDelegate;

        public Mapper(ILogger logger, IFileSystemService fileSystemService, MapDelegate mapDelegate)
        {
            this.logger = logger;
            this.fileSystemService = fileSystemService;
            this.mapDelegate = mapDelegate;
        }

        public void Map(string readDataFilePath, string writeDataDirectoryPath)
        {
            logger.Log($"Started map phase, reading data from path: {readDataFilePath}");

            try
            {
                List<KeyValuePair<string, string>> data = mapDelegate(logger, readDataFilePath);
                string tempFilePath = Path.Combine(writeDataDirectoryPath, $"{Guid.NewGuid():N}.txt");
                fileSystemService.WriteCollectionToPath(tempFilePath, data, entry => $"{entry.Key}:{entry.Value}");
            }
            catch (Exception ex)
            {
                logger.Error($"Error in mapping: {ex.Message}");
            }

            logger.Log("Finished map phase");
        }
    }
}

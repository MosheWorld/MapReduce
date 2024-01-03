using MapReduce.Logger;
using MapReduce.Mapper;
using MapReduce.Reducer;
using MapReduce.Shuffler;
using MapReduce.FileSystemService;

namespace MapReduce.MapReduceManager
{
    // The main class responsible for managing the MapReduce process
    public class MapReduceManager : IMapReduceManager
    {
        private readonly IMapper mapper;
        private readonly IShuffler shuffler;
        private readonly IReducer reducer;
        private readonly IFileSystemService fileSystemService;
        private readonly ILogger logger;

        public MapReduceManager(IMapper mapper, IShuffler shuffler, IReducer reducer, IFileSystemService fileSystemService, ILogger logger)
        {
            this.mapper = mapper;
            this.shuffler = shuffler;
            this.reducer = reducer;
            this.fileSystemService = fileSystemService;
            this.logger = logger;
        }

        public List<KeyValuePair<string, int>> RunMapReduce(string inputDataDirectoryPath, string mapResultsDirectoryPath, string shuffleResultsDirectoryPath, string reduceResultsDirectoryPath)
        {
            try
            {
                // Get the list of log files from the specified directory
                List<string> files = fileSystemService.GetFilePathsFromDirectory(inputDataDirectoryPath);

                // Use parallel processing to execute mapping function for each file
                Parallel.ForEach(files, file => mapper.Map(file, mapResultsDirectoryPath));

                // Shuffle the mapped results
                shuffler.Shuffle(mapResultsDirectoryPath, shuffleResultsDirectoryPath);

                // Get the list of files generated during shuffle phase
                List<string> generatedFilePathsByShuffle = Directory.GetFiles(shuffleResultsDirectoryPath).ToList();

                // Use parallel processing to execute reduction function for each file
                Parallel.ForEach(generatedFilePathsByShuffle, filePathByShuffle => reducer.Reduce(filePathByShuffle, reduceResultsDirectoryPath));

                // Get the list of files generated during reduce phase
                List<string> generatedFilePathsByReduce = Directory.GetFiles(reduceResultsDirectoryPath).ToList();

                // Get data from reduce files
                return GetSongCount(generatedFilePathsByReduce);
            }
            catch (Exception ex)
            {
                // Log any errors that occur during the MapReduce process
                logger.Error($"Error during MapReduce: {ex.Message}");
                return new List<KeyValuePair<string, int>>();
            }
            finally
            {
                // Clean up the temporary files, regardless of success or failure
                fileSystemService.CleanUpDirectory(mapResultsDirectoryPath);
                fileSystemService.CleanUpDirectory(shuffleResultsDirectoryPath);
                fileSystemService.CleanUpDirectory(reduceResultsDirectoryPath);
            }
        }

        private List<KeyValuePair<string, int>> GetSongCount(List<string> filePaths)
        {
            List<KeyValuePair<string, int>> songCounts = new();

            foreach (string filePath in filePaths)
            {
                string text = File.ReadAllText(filePath);

                // Split each line into parts based on ':' delimiter
                string[] parts = text.Split(':');
                string songName = parts[0];
                int songOccurrence = int.Parse(parts[1]);
                songCounts.Add(new KeyValuePair<string, int>(songName, songOccurrence));
            }

            return songCounts;
        }
    }
}

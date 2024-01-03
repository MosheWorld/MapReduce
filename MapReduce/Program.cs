using MapReduce.Logger;
using MapReduce.Mapper;
using MapReduce.Reducer;
using MapReduce.Shuffler;
using MapReduce.MapReduceManager;
using MapReduce.FileSystemService;

public class Program
{
    static void Main()
    {
        // Initialize logger for recording MapReduce logs
        ILogger logger = new Logger("MapReduceLogFile.txt");

        // Create instances of the Mapper, Reducer, FileReader, and MapReduceManager
        IFileSystemService fileSystemService = new FileSystemService(logger);
        IMapper mapper = new Mapper(logger, fileSystemService, MapDelegate);
        IShuffler shuffler = new Shuffler(logger, fileSystemService, ShuffleDelegate);
        IReducer reducer = new Reducer(logger, fileSystemService, ReducerDelegate);
        IMapReduceManager mapReduceManager = new MapReduceManager(mapper, shuffler, reducer, fileSystemService, logger);

        // Define paths for log and temporary directories
        // Code is executed inside bin directory, the Logs and Temp directories are stored 3 levels above the execute directory of .NET
        string inputDataDirectoryPath = "../../../Logs";
        string mapTempDirectoryPath = "../../../Map Temporarily Files";
        string shuffleTempDirectoryPath = "../../../Shuffle Temporarily Files";
        string reduceTempDirectoryPath = "../../../Reduce Temporarily Files";

        // Run MapReduce process and get song counts
        List<KeyValuePair<string, int>> songCounts = mapReduceManager.RunMapReduce(inputDataDirectoryPath, mapTempDirectoryPath, shuffleTempDirectoryPath, reduceTempDirectoryPath);

        // Find and output the top 5 most played songs
        IEnumerable<KeyValuePair<string, int>> topSongs = songCounts.OrderByDescending(pair => pair.Value).Take(5);

        Console.WriteLine("Top 5 Most Played Songs:");

        // Display the top songs and their play counts
        foreach (KeyValuePair<string, int> song in topSongs)
            Console.WriteLine($"{song.Key}: {song.Value} plays");
    }

    public static List<KeyValuePair<string, string>> MapDelegate(ILogger logger, string readDataFilePath)
    {
        List<KeyValuePair<string, string>> songs = new();
        foreach (string text in File.ReadLines(readDataFilePath))
        {
            try
            {
                // Assuming log lines are in the format: <song_name> played, extract the song name from the line
                string songName = text.Split(' ')[0];

                // Update the song count in the dictionary
                songs.Add(new KeyValuePair<string, string>(songName, "1"));
            }
            catch
            {
                logger.Error($"An error occurred when attempt to get a song name at the text: {text}");
            }
        }

        return songs;
    }

    public static List<KeyValuePair<string, string>> ShuffleDelegate(ILogger logger, List<string> filePaths)
    {
        List<KeyValuePair<string, string>> songs = new();
        foreach (string filePath in filePaths)
        {
            logger.Log($"Reading the filePath: ${filePath} at shuffle phase");

            foreach (string text in File.ReadLines(filePath))
            {
                // Split each line into parts based on ':' delimiter
                string[] parts = text.Split(':');
                string songName = parts[0];
                string songOccurrence = parts[1];
                songs.Add(new KeyValuePair<string, string>(songName, songOccurrence));
            }
        }

        List<KeyValuePair<string, string>> songsOccurrences = new();
        foreach (IGrouping<string, KeyValuePair<string, string>> group in songs.GroupBy(item => item.Key))
        {
            string songName = group.Key;
            string songOccurrences = string.Join(",", group.Select(item => item.Value).ToList());
            songsOccurrences.Add(new KeyValuePair<string, string>(songName, songOccurrences));
            logger.Log($"Successfully constructed a new pair with the song name: ${songName} and grouped the occurrences");
        }

        return songsOccurrences;
    }

    public static KeyValuePair<string, string> ReducerDelegate(ILogger logger, string readDataFilePath)
    {
        // Dictionary to store the reduced song counts
        Dictionary<string, int> reducedSongOccurrences = new();

        // Read each line from the temp file
        logger.Log($"Attempt to read the file: ${readDataFilePath}");
        string text = File.ReadAllText(readDataFilePath);

        // Split each line into parts based on ':' delimiter
        string[] parts = text.Split(":");
        string songName = parts[0];
        List<int> occurrences = parts[1].Split(",").Select(e => int.Parse(e)).ToList();

        logger.Log($"Successfully constructed a new pair with the song name: ${songName} and summed the occurrences");
        return new KeyValuePair<string, string>(songName, occurrences.Sum().ToString());
    }
}

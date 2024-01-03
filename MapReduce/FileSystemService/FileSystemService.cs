using MapReduce.Logger;

namespace MapReduce.FileSystemService
{
    public class FileSystemService : IFileSystemService
    {
        private readonly ILogger logger;

        public FileSystemService(ILogger logger)
        {
            this.logger = logger;
        }

        public List<string> GetFilePathsFromDirectory(string directoryPath)
        {
            try
            {
                return Directory.GetFiles(directoryPath).ToList();
            }
            catch (Exception ex)
            {
                logger.Error($"Error while reading log files: {ex.Message}");
                return new List<string>();
            }
        }

        public void WriteCollectionToPath<T>(string filePath, IEnumerable<T> data, Func<T, string> formatEntry)
        {
            CreateDirectoryIfNotExist(filePath);

            using (StreamWriter writer = new(filePath))
            {
                foreach (T? entry in data)
                    writer.WriteLine(formatEntry(entry));
            }
        }

        public void WriteKeyValuePairToPath(string filePath, KeyValuePair<string, string> data)
        {
            CreateDirectoryIfNotExist(filePath);

            using (StreamWriter writer = new(filePath))
            {
                writer.WriteLine($"{data.Key}:{data.Value}");
            }
        }

        public void CleanUpDirectory(string directoryPath)
        {
            try
            {
                // Get all files in the directory
                string[] tempFiles = Directory.GetFiles(directoryPath);

                // Iterate through each file
                foreach (string tempFilePath in tempFiles)
                {
                    try
                    {
                        logger.Log($"Deleting temp file: {tempFilePath}");

                        // Open the file, truncate its content, and then delete it
                        using (FileStream fs = new(tempFilePath, FileMode.Open, FileAccess.Write))
                        {
                            fs.SetLength(0);
                        }

                        File.Delete(tempFilePath);
                    }
                    catch (Exception ex)
                    {
                        // Log any errors that occur during file deletion
                        logger.Error($"Error deleting temp file {tempFilePath}: {ex.Message}");
                    }
                }

                // Remove the directory itself
                Directory.Delete(directoryPath);
                logger.Log($"Deleted directory: {directoryPath}");
            }
            catch (Exception ex)
            {
                // Log any errors that occur during directory deletion
                logger.Error($"Error deleting directory {directoryPath}: {ex.Message}");
            }
        }


        private void CreateDirectoryIfNotExist(string filePath)
        {
            string? directoryPath = Path.GetDirectoryName(filePath);
            if (!string.IsNullOrEmpty(directoryPath) && !Directory.Exists(directoryPath))
                Directory.CreateDirectory(directoryPath);
        }
    }
}

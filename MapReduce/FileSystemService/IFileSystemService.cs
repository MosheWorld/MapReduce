namespace MapReduce.FileSystemService
{
    public interface IFileSystemService
    {
        List<string> GetFilePathsFromDirectory(string directoryPath);
        void WriteCollectionToPath<T>(string filePath, IEnumerable<T> data, Func<T, string> formatEntry);
        void WriteKeyValuePairToPath(string filePath, KeyValuePair<string, string> data);
        public void CleanUpDirectory(string directoryPath);
    }
}

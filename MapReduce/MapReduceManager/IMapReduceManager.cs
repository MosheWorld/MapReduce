namespace MapReduce.MapReduceManager
{
    public interface IMapReduceManager
    {
        List<KeyValuePair<string, int>> RunMapReduce(string inputDataDirectoryPath, string mapResultsDirectoryPath, string shuffleResultsDirectoryPath, string reduceResultsDirectoryPath);
    }
}

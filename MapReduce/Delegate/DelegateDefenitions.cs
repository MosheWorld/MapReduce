using MapReduce.Logger;

namespace MapReduce.DelegateDefenitions
{
    public delegate List<KeyValuePair<string, string>> MapDelegate(ILogger logger, string readDataFilePath);
    public delegate List<KeyValuePair<string, string>> ShuffleDelegate(ILogger logger, List<string> filePaths);
    public delegate KeyValuePair<string, string> ReducerDelegate(ILogger logger, string readDataFilePath);
}

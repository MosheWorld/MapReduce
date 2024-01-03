namespace MapReduce.Reducer
{
    public interface IReducer
    {
        void Reduce(string readDataFilePath, string writeDataDirectoryPath);
    }
}

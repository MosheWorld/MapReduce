namespace MapReduce.Shuffler
{
    public interface IShuffler
    {
        void Shuffle(string readDataDirectoryPath, string writeDataDirectoryPath);
    }
}

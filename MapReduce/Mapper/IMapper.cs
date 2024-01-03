namespace MapReduce.Mapper
{
    public interface IMapper
    {
        void Map(string readDataFilePath, string writeDataDirectoryPath);
    }
}

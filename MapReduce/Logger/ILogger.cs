namespace MapReduce.Logger
{
    public interface ILogger
    {
        void Log(string message);
        void Error(string message);
    }
}

using System.Collections.Concurrent;

namespace MapReduce.Logger
{
    public class Logger : ILogger
    {
        private readonly string logFilePath;  // Path to the log file
        private readonly ConcurrentQueue<string> messageQueue;  // Queue to store log messages
        private readonly CancellationTokenSource cancellationTokenSource;  // Token source for cancelling logging task
        private readonly Task loggingTask;  // Task for processing the log queue

        public Logger(string logFilePath)
        {
            this.logFilePath = logFilePath;
            messageQueue = new ConcurrentQueue<string>();
            cancellationTokenSource = new CancellationTokenSource();

            // Start a background task to process the log queue
            loggingTask = Task.Run(() => ProcessLogQueue(cancellationTokenSource.Token));
        }

        public void Log(string message)
        {
            EnqueueLogMessage($"[Log] [{GetTimestamp()}] - {message}");
        }

        public void Error(string message)
        {
            EnqueueLogMessage($"[Error] [{GetTimestamp()}] - {message}");
        }

        // Enqueues a log message with thread information
        private void EnqueueLogMessage(string logMessage)
        {
            string threadInfo = $"[Thread {Thread.CurrentThread.ManagedThreadId}]";
            messageQueue.Enqueue($"{threadInfo} {logMessage}");
        }

        private string GetTimestamp()
        {
            return DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
        }

        // Processes the log queue in a background task
        private void ProcessLogQueue(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (messageQueue.TryDequeue(out string? logMessage))
                {
                    WriteToLog(logMessage);
                }
                else
                {
                    Thread.Sleep(10);  // Sleep for a short duration if the queue is empty
                }
            }
        }

        // Writes a log message to the log file in a thread-safe manner
        private void WriteToLog(string logMessage)
        {
            try
            {
                lock (this)
                {
                    File.AppendAllText(logFilePath, logMessage + Environment.NewLine);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error while writing to log file: {ex.Message}");
            }
        }

        // Disposes of resources and cancels the logging task
        public void Dispose()
        {
            cancellationTokenSource.Cancel();
            loggingTask.Wait();  // Wait for the logging task to complete
            cancellationTokenSource.Dispose();
        }
    }
}

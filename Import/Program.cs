using Import.ConnectionSupport;

namespace Import;

// !Ta klasa nie podlega code review!
class Program
{
    static void Main(string[] args)
    {
        while (true)
        {
            var connectionSupport = RetryUntilSuccess(SelectConnectorToImport);
            var connectionDetails = RetryUntilSuccess(connectionSupport.GetConnectionDetailsForSelection);
            var selectedToImport = RetryUntilSuccess(() =>
                connectionSupport.SelectToImport(connectionDetails)
            );
            var connection = connectionSupport.GetConnection(connectionDetails, selectedToImport);
            Console.Clear();
            try
            {
                connectionSupport.RunImport(connection);
                Console.WriteLine("Import completed successfully.");
            }
            finally
            {
                connectionSupport.CloseConnection(connection);
            }
            Console.WriteLine("Press any key to continue, or 'q' to exit...");
            var key = Console.ReadKey(true);
            if (key.KeyChar is 'q' or 'Q')
            {
                Environment.Exit(0);
            }
        }
    }

    private static T RetryUntilSuccess<T>(Func<T> action)
    {
        string? errorMessage = null;

        while (true)
        {
            Console.Clear();
            if (!string.IsNullOrEmpty(errorMessage))
            {
                Console.WriteLine($"Error: {errorMessage}");
            }

            try
            {
                return action();
            }
            catch (Exception ex)
            {
                errorMessage = ex.Message;
            }
        }
    }

    private static IConnectionSupport SelectConnectorToImport()
    {
        Console.WriteLine("Select a connector to import from:");
        Console.WriteLine("1. Fivetran");
        Console.Write("Enter the number of your choice: ");

        return Console.ReadLine() switch
        {
            "1" => ConnectionSupportFactory.GetConnectionSupport(FivetranConnectionSupport.CONNECTOR_TYPE_CODE),
            _ => throw new NotSupportedException("This connector is not supported yet.")
        };
    }
}
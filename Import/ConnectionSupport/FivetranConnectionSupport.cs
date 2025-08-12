using System.Collections.Concurrent;
using System.Text;
using FivetranClient;
using Import.Helpers.Fivetran;

namespace Import.ConnectionSupport;

// equivalent of database is group in Fivetran terminology
public class FivetranConnectionSupport : IConnectionSupport
{
  public const string CONNECTOR_TYPE_CODE = "FIVETRAN";

  private record FivetranConnectionDetailsForSelection(string ApiKey, string ApiSecret);

  public object? GetConnectionDetailsForSelection()
  {
    Console.Write("Provide your Fivetran API Key: ");
    var apiKey = Console.ReadLine() ?? throw new ArgumentNullException();
    Console.Write("Provide your Fivetran API Secret: ");
    var apiSecret = Console.ReadLine() ?? throw new ArgumentNullException();

    return new FivetranConnectionDetailsForSelection(apiKey, apiSecret);
  }

  public object GetConnection(object? connectionDetails, string? selectedToImport)
  {
    ArgumentNullException.ThrowIfNull(selectedToImport);
    if (connectionDetails is not FivetranConnectionDetailsForSelection details)
    {
      throw new ArgumentException("Invalid connection details provided.");
    }

    return new RestApiManagerWrapper(
      new RestApiManager(details.ApiKey, details.ApiSecret, TimeSpan.FromSeconds(40)),
      selectedToImport
    );
  }

  public void CloseConnection(object? connection)
  {
    switch (connection)
    {
      case RestApiManager restApiManager:
        restApiManager.Dispose();
        break;
      case RestApiManagerWrapper restApiManagerWrapper:
        restApiManagerWrapper.Dispose();
        break;
      default:
        throw new ArgumentException("Invalid connection type provided.");
    }
  }

  public string SelectToImport(object? connectionDetails)
  {
    if (connectionDetails is not FivetranConnectionDetailsForSelection details)
    {
      throw new ArgumentException("Invalid connection details provided.");
    }

    using var restApiManager = new RestApiManager(
      details.ApiKey,
      details.ApiSecret,
      TimeSpan.FromSeconds(40)
    );
    var groups = restApiManager.GetGroupsAsync(CancellationToken.None).ToBlockingEnumerable().ToArray();

    if (groups.Length == 0)
    {
      throw new Exception("No groups found in Fivetran account.");
    }

    // bufforing for performance
    var consoleOutputBuffer = "";
    consoleOutputBuffer += "Available groups in Fivetran account:\n";
    var elementIndex = 1;
    foreach (var group in groups)
    {
      consoleOutputBuffer += $"{elementIndex++}. {group.Name} (ID: {group.Id})\n";
    }
    consoleOutputBuffer += "Please select a group to import from (by number): ";
    Console.Write(consoleOutputBuffer);

    var input = Console.ReadLine();
    if (
      string.IsNullOrWhiteSpace(input)
      || !int.TryParse(input, out var selectedIndex)
      || selectedIndex < 1
      || selectedIndex > groups.Length
    )
    {
      throw new ArgumentException("Invalid group selection.");
    }

    // user selection index starts with 1 not 0
    var selectedGroup = groups.ElementAt(selectedIndex - 1);
    return selectedGroup.Id;
  }

  public void RunImport(object? connection)
  {
    if (connection is not RestApiManagerWrapper restApiManagerWrapper)
    {
      throw new ArgumentException("Invalid connection type provided.");
    }

    var restApiManager = restApiManagerWrapper.RestApiManager;
    var groupId = restApiManagerWrapper.GroupId;

    var connectors = restApiManager
      .GetConnectorsAsync(groupId, CancellationToken.None)
      .ToBlockingEnumerable()
      .ToArray();

    if (connectors.Length == 0)
    {
      throw new Exception("No connectors found in the selected group.");
    }

    var mappings = new ConcurrentBag<string>();
    Parallel.ForEach(
      connectors,
      connector =>
      {
        var connectorSchemas = restApiManager
          .GetConnectorSchemasAsync(connector.Id, CancellationToken.None)
          .Result;

        foreach (var schema in connectorSchemas?.Schemas ?? [])
        {
          foreach (var table in schema.Value?.Tables ?? [])
          {
            mappings.Add(
              $"  {connector.Id}: {schema.Key}.{table.Key} -> {schema.Value?.NameInDestination}.{table.Value.NameInDestination}\n"
            );
          }
        }
      }
    );

    var stringBuilder = new StringBuilder();
    stringBuilder.EnsureCapacity(mappings.Count * 85); // start with (size of line * AVG lenght of mapping string)
    stringBuilder.Append("Lineage mappings:\n");
    foreach (var mapping in mappings)
    {
      stringBuilder.Append(mapping);
    }

    Console.WriteLine(stringBuilder.ToString());
  }
}

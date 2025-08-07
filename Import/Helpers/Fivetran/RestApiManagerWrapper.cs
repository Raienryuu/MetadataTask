using FivetranClient;

namespace Import.Helpers.Fivetran;

public class RestApiManagerWrapper(RestApiManager restApiManager, string groupId) : IDisposable
{
  public RestApiManager RestApiManager { get; } = restApiManager;
  public string GroupId { get; } = groupId;

  public void Dispose()
  {
    RestApiManager.Dispose();
    GC.SuppressFinalize(this);
  }
}

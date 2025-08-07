using System.Text.Json;
using FivetranClient.Models;

namespace FivetranClient.Fetchers;

public sealed class NonPaginatedFetcher(HttpRequestHandler requestHandler) : BaseFetcher(requestHandler)
{
  public async Task<T?> FetchAsync<T>(string endpoint, CancellationToken cancellationToken)
  {
    var response = await base.RequestHandler.GetAsync(endpoint, cancellationToken);
    var content = await response.Content.ReadAsStringAsync(cancellationToken);
    var root = JsonSerializer.Deserialize<NonPaginatedRoot<T>>(content, SerializerOptions);
    return root is null ? default(T) : root.Data;
  }
}

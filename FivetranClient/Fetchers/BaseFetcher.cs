using System.Text.Json;

namespace FivetranClient.Fetchers;

public abstract class BaseFetcher(HttpRequestHandler requestHandler)
{
  protected readonly HttpRequestHandler RequestHandler = requestHandler;

  protected static readonly JsonSerializerOptions SerializerOptions = new JsonSerializerOptions
  {
    PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower, PropertyNameCaseInsensitive = true,
  };
}

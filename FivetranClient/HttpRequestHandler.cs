using System.Net;
using FivetranClient.Infrastructure;

namespace FivetranClient;

public class HttpRequestHandler
{
  private readonly HttpClient _client;
  private readonly SemaphoreSlim? _semaphore;
  private readonly object _lock = new();
  private DateTime _retryAfterTime = DateTime.UtcNow;
  private static readonly TtlDictionary<string, HttpResponseMessage> s_responseCache = new();

  /// <summary>
  /// Handles HttpTooManyRequests responses by limiting the number of concurrent requests and managing retry logic.
  /// Also caches responses to avoid unnecessary network calls.
  /// </summary>
  /// <remarks>
  /// Set <paramref name="maxConcurrentRequests"/> to 0 to disable concurrency limit.
  /// </remarks>
  public HttpRequestHandler(HttpClient client, ushort maxConcurrentRequests = 0)
  {
    _client = client;
    if (maxConcurrentRequests > 0)
    {
      _semaphore = new SemaphoreSlim(0, maxConcurrentRequests);
    }
  }

  public Task<HttpResponseMessage> GetAsync(string url, CancellationToken cancellationToken)
  {
    return s_responseCache.GetOrAdd(
      url,
      async () => await GetAsyncWithoutCache(url, cancellationToken),
      TimeSpan.FromMinutes(60)
    );
  }

  private async Task<HttpResponseMessage> GetAsyncWithoutCache(
    string url,
    CancellationToken cancellationToken
  )
  {
    if (_semaphore is not null)
    {
      await _semaphore.WaitAsync(cancellationToken);
    }

    TimeSpan timeToWait;
    HttpResponseMessage response = new();

    while (!cancellationToken.IsCancellationRequested)
    {
      lock (_lock)
      {
        timeToWait = _retryAfterTime - DateTime.UtcNow;
      }

      if (timeToWait > TimeSpan.Zero)
      {
        await Task.Delay(timeToWait, cancellationToken);
      }

      cancellationToken.ThrowIfCancellationRequested();

      response = await _client.GetAsync(new Uri(url, UriKind.Relative), cancellationToken);
      if (response.StatusCode is HttpStatusCode.TooManyRequests)
      {
        var retryAfter = response.Headers.RetryAfter?.Delta ?? TimeSpan.FromSeconds(60);

        lock (_lock)
        {
          // new request will wait for the specified time before retrying
          _retryAfterTime = DateTime.UtcNow.Add(retryAfter);
        }
      }
      else
      {
        response.EnsureSuccessStatusCode();
        break;
      }
    }

    _semaphore?.Release();
    return response;
  }
}

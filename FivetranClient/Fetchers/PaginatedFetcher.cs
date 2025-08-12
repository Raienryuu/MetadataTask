using System.Net;
using System.Runtime.CompilerServices;
using System.Text.Json;
using FivetranClient.Models;

namespace FivetranClient.Fetchers;

public sealed class PaginatedFetcher(HttpRequestHandler requestHandler) : BaseFetcher(requestHandler)
{
  private const ushort PAGE_SIZE = 100;

  public IAsyncEnumerable<T> FetchItemsAsync<T>(string endpoint, CancellationToken cancellationToken)
  {
    var firstPageTask = FetchPageAsync<T>(endpoint, cancellationToken);
    return ProcessPagesIterativelyAsync(endpoint, firstPageTask, cancellationToken);
  }

  private async Task<PaginatedRoot<T>?> FetchPageAsync<T>(
    string endpoint,
    CancellationToken cancellationToken,
    string? cursor = null
  )
  {
    var response = cursor is null
      ? await RequestHandler.GetAsync($"{endpoint}?limit={PAGE_SIZE}", cancellationToken)
      : await RequestHandler.GetAsync(
        $"{endpoint}?limit={PAGE_SIZE}&cursor={WebUtility.UrlEncode(cursor)}",
        cancellationToken
      );
    var content = await response.Content.ReadAsStringAsync(cancellationToken);
    return JsonSerializer.Deserialize<PaginatedRoot<T>>(content, SerializerOptions);
  }

  /// This implementation provides items as soon as they are available but also in the meantime fetches the next page
  private async IAsyncEnumerable<T> ProcessPagesIterativelyAsync<T>(
    string endpoint,
    Task<PaginatedRoot<T>?> currentPageTask,
    [EnumeratorCancellation] CancellationToken cancellationToken
  )
  {
    cancellationToken.ThrowIfCancellationRequested();
    var currentPage = await currentPageTask;
    var nextCursor = currentPage?.Data?.NextCursor;

    do
    {
      var nextTask = !string.IsNullOrWhiteSpace(nextCursor) ? FetchPageAsync<T>(endpoint, cancellationToken, nextCursor) : null;

      foreach (var item in currentPage?.Data?.Items ?? [])
      {
        cancellationToken.ThrowIfCancellationRequested();
        yield return item;
      }

      if (nextTask is null)
      {
        yield break;
      }

      currentPage = await nextTask;
      nextCursor = currentPage?.Data.NextCursor;

    } while (!cancellationToken.IsCancellationRequested || nextCursor is null);
  }
}

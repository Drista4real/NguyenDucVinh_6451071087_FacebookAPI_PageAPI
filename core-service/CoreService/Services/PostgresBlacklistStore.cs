using CoreService.Models;
using Microsoft.Extensions.Options;
using Npgsql;

namespace CoreService.Services;

public class PostgresBlacklistStore : IBlacklistStore, IAsyncDisposable
{
    private readonly BlacklistOptions _options;
    private readonly ILogger<PostgresBlacklistStore> _logger;
    private readonly NpgsqlDataSource? _dataSource;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private bool _initialized;

    public PostgresBlacklistStore(
        IOptions<BlacklistOptions> options,
        ILogger<PostgresBlacklistStore> logger)
    {
        _options = options.Value;
        _logger = logger;

        if (!string.IsNullOrWhiteSpace(_options.ConnectionString))
        {
            _dataSource = NpgsqlDataSource.Create(_options.ConnectionString);
        }
    }

    public async Task<bool> IsBlacklistedAsync(string userId, CancellationToken cancellationToken)
    {
        if (_dataSource is null)
        {
            return false;
        }

        await EnsureInitializedAsync(cancellationToken);

        await using var command = _dataSource.CreateCommand("""
            select violation_count >= $1
            from user_blacklist
            where user_id = $2
            """);
        command.Parameters.AddWithValue(_options.MaxViolationsBeforeBlacklist);
        command.Parameters.AddWithValue(userId);

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is bool isBlacklisted && isBlacklisted;
    }

    public async Task<int> IncrementViolationAsync(
        string userId,
        string reason,
        CancellationToken cancellationToken)
    {
        if (_dataSource is null)
        {
            _logger.LogWarning("Blacklist persistence is disabled because Blacklist:ConnectionString is missing.");
            return 1;
        }

        await EnsureInitializedAsync(cancellationToken);

        await using var command = _dataSource.CreateCommand("""
            insert into user_blacklist (user_id, violation_count, reason, updated_at)
            values ($1, 1, $2, now())
            on conflict (user_id)
            do update set
                violation_count = user_blacklist.violation_count + 1,
                reason = excluded.reason,
                updated_at = now()
            returning violation_count
            """);
        command.Parameters.AddWithValue(userId);
        command.Parameters.AddWithValue(reason);

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt32(result);
    }

    private async Task EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        if (_initialized || _dataSource is null)
        {
            return;
        }

        await _initLock.WaitAsync(cancellationToken);
        try
        {
            if (_initialized)
            {
                return;
            }

            await using var command = _dataSource.CreateCommand("""
                create table if not exists user_blacklist (
                    user_id text primary key,
                    violation_count integer not null default 0,
                    reason text,
                    updated_at timestamptz not null default now()
                )
                """);
            await command.ExecuteNonQueryAsync(cancellationToken);
            _initialized = true;
        }
        finally
        {
            _initLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        _initLock.Dispose();

        if (_dataSource is not null)
        {
            await _dataSource.DisposeAsync();
        }
    }
}

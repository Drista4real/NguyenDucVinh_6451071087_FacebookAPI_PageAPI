using Microsoft.Extensions.Options;
using Npgsql;
using Page_API.Models;

namespace Page_API.Services
{
    public class PostgresProcessedCommandStore : IProcessedCommandStore, IAsyncDisposable
    {
        private readonly ProcessedCommandOptions _options;
        private readonly ILogger<PostgresProcessedCommandStore> _logger;
        private readonly NpgsqlDataSource? _dataSource;
        private readonly SemaphoreSlim _initLock = new(1, 1);
        private bool _initialized;

        public PostgresProcessedCommandStore(
            IOptions<ProcessedCommandOptions> options,
            ILogger<PostgresProcessedCommandStore> logger)
        {
            _options = options.Value;
            _logger = logger;

            if (!string.IsNullOrWhiteSpace(_options.ConnectionString))
            {
                _dataSource = NpgsqlDataSource.Create(_options.ConnectionString);
            }
        }

        public async Task<bool> IsProcessedAsync(string idempotencyKey, CancellationToken cancellationToken)
        {
            if (_dataSource is null)
            {
                _logger.LogWarning("Idempotency store disabled because ProcessedCommands:ConnectionString is missing.");
                return false;
            }

            await EnsureInitializedAsync(cancellationToken);

            await using var command = _dataSource.CreateCommand("""
                select exists(
                    select 1
                    from processed_commands
                    where idempotency_key = $1
                )
                """);
            command.Parameters.AddWithValue(idempotencyKey);

            var result = await command.ExecuteScalarAsync(cancellationToken);
            return result is bool exists && exists;
        }

        public async Task MarkProcessedAsync(
            string idempotencyKey,
            string commandId,
            string action,
            CancellationToken cancellationToken)
        {
            if (_dataSource is null)
            {
                return;
            }

            await EnsureInitializedAsync(cancellationToken);

            await using var command = _dataSource.CreateCommand("""
                insert into processed_commands (idempotency_key, command_id, action, processed_at)
                values ($1, $2, $3, now())
                on conflict (idempotency_key) do nothing
                """);
            command.Parameters.AddWithValue(idempotencyKey);
            command.Parameters.AddWithValue(commandId);
            command.Parameters.AddWithValue(action);

            await command.ExecuteNonQueryAsync(cancellationToken);
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
                    create table if not exists processed_commands (
                        idempotency_key text primary key,
                        command_id text not null,
                        action text not null,
                        processed_at timestamptz not null default now()
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
}

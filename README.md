# Facebook Page API Microservices

He thong gom 4 service giao tiep qua Kafka theo `Docs/System_design.md`:

- `webhook-service` port `3001`: nhan Facebook webhook, verify HMAC, normalize event, publish `raw_events`.
- `core-service` port `3002`: consume `raw_events`, phan tich intent/sentiment, chay automation rule, publish `reply_commands`.
- `backend-api` port `3000`: REST API va executor duy nhat goi Facebook Graph API, consume `reply_commands` + `send_retry`, publish `send_failed` khi loi tam thoi.
- `retry-service` port `3003`: consume `send_failed`, exponential backoff, publish `send_retry` hoac `dead_letter`.

## Infrastructure

```powershell
docker compose up -d
```

Kafka UI: `http://localhost:8080`

PostgreSQL mac dinh:

- Host: `localhost`
- Port: `5432`
- Database: `pageapi`
- User/password: `postgres` / `postgres`

## Run Services

Mo 4 terminal rieng:

```powershell
dotnet run --project webhook-service/WebhookService/WebhookService.csproj --launch-profile WebhookService
dotnet run --project core-service/CoreService/CoreService.csproj --launch-profile http
dotnet run --project backend-api/BackendApi/BackendApi.csproj --launch-profile http
dotnet run --project retry-service/RetryService/RetryService.csproj --launch-profile RetryService
```

Health checks:

```powershell
Invoke-WebRequest http://localhost:3001/health -UseBasicParsing
Invoke-WebRequest http://localhost:3002/health -UseBasicParsing
Invoke-WebRequest http://localhost:3003/health -UseBasicParsing
```

## Kafka Topics

`docker-compose.yml` tu tao cac topic:

- `raw_events`
- `reply_commands`
- `send_retry`
- `send_failed`
- `dead_letter`

## Build

```powershell
dotnet build Page-API.sln
```

## Notes

Config Facebook token/app secret dang nam trong `appsettings.json` de chay bai local. Khi dua len moi truong that, nen chuyen sang user-secrets hoac environment variables.

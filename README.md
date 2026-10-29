# Payments processing

Сервис асинхронной обработки платежей.

## Запуск

```
docker compose up -d --build
```

API: http://localhost:8000/docs  
RabbitMQ: http://localhost:15672 (guest/guest)

Миграции накатываются при старте api. API ключ по умолчанию `change-me` (переменная `API_KEY`).

Для проверки вебхуков в compose есть `webhook-receiver`, смотреть через `docker compose logs -f webhook-receiver`.

## Примеры

Создать платеж:

```
curl -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: change-me" \
  -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{"amount": "1500.50", "currency": "RUB", "description": "Order 42", "metadata": {"order_id": 42}, "webhook_url": "http://webhook-receiver:8080/hook"}'
```

Получить платеж:

```
curl http://localhost:8000/api/v1/payments/<payment_id> -H "X-API-Key: change-me"
```

Повторный запрос с тем же Idempotency-Key вернет тот же платеж, с другим телом - 409.

## Как работает

- платеж и событие в outbox пишутся в одной транзакции, api в фоне публикует события из outbox в очередь `payments.new`
- consumer обрабатывает платеж, обновляет статус и шлет вебхук
- при ошибке сообщение уходит в retry очередь с задержкой 2с, 4с, после 3 попыток в `payments.new.dlq`

# Crypto Indices Data Collection System

Автоматическая система для сбора и анализа данных о криптовалютных индексах с помощью LunarCrush API и сохранения в Supabase.

## 📊 Описание

Система автоматически собирает данные каждые 30 минут и рассчитывает индексы для трех секторов:

- **RWA (Real World Assets)** - токены, связанные с реальными активами
- **DEFI** - протоколы децентрализованных финансов  
- **MEME** - мем-токены и community-driven проекты

## 🚀 Быстрый старт

### 1. Клонирование репозитория

```bash
git clone https://github.com/yourusername/crypto-indices.git
cd crypto-indices
```

### 2. Установка зависимостей

```bash
pip install -r requirements.txt
```

### 3. Настройка переменных окружения

Создайте файл `.env` в корневой папке проекта:

```bash
# LunarCrush API
LUNARCRUSH_API_KEY=your_api_key_here

# Supabase
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_anon_key

# PostgreSQL (если используется прямое подключение)
POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_HOST=your_postgres_host
POSTGRES_PORT=5432
POSTGRES_DATABASE=postgres
```

### 4. Локальный запуск

```bash
python main.py
```

## 🔧 Настройка GitHub Actions

### Добавление Secrets в GitHub

1. Перейдите в ваш репозиторий на GitHub
2. Нажмите **Settings** → **Secrets and variables** → **Actions**
3. Добавьте следующие secrets:

#### Обязательные secrets:

- `LUNARCRUSH_API_KEY` - ваш API ключ от LunarCrush
- `SUPABASE_URL` - URL вашего Supabase проекта
- `SUPABASE_KEY` - анонимный ключ Supabase
- `POSTGRES_USER` - имя пользователя PostgreSQL
- `POSTGRES_PASSWORD` - пароль PostgreSQL
- `POSTGRES_HOST` - хост PostgreSQL
- `POSTGRES_PORT` - порт PostgreSQL (обычно 5432)
- `POSTGRES_DATABASE` - имя базы данных

### Пример добавления secret:

1. Нажмите **New repository secret**
2. **Name**: `LUNARCRUSH_API_KEY`
3. **Secret**: `ваш_api_ключ`
4. Нажмите **Add secret**

Повторите для всех secrets из списка выше.

## 🏗️ Структура проекта

```
crypto-indices/
├── main.py                    # Основной скрипт
├── requirements.txt           # Python зависимости
├── README.md                  # Документация
├── .gitignore                # Файлы для игнорирования Git
├── crypto_indices.log        # Лог-файл (создается автоматически)
└── .github/
    └── workflows/
        └── data_collection.yml # GitHub Actions workflow
```

## 📈 Как работает система

1. **Сбор данных**: Каждые 30 минут система запрашивает данные у LunarCrush API
2. **Обработка**: Фильтрует токены по секторам и рассчитывает нормализованные метрики
3. **Расчет индексов**: Вычисляет взвешенные индексы по 4 параметрам:
   - Sentiment (25%)
   - Interactions 24h (25%)
   - Social volume 24h (25%)
   - Alt rank (25%)
4. **Сохранение**: Записывает результаты в Supabase
5. **Логирование**: Ведет подробные логи выполнения

## 🔍 Мониторинг

### Проверка работы GitHub Actions:

1. Перейдите в **Actions** в вашем репозитории
2. Выберите последний запуск **Crypto Indices Data Collection**
3. Посмотрите логи выполнения

### Ручной запуск:

1. Перейдите в **Actions** → **Crypto Indices Data Collection**
2. Нажмите **Run workflow** → **Run workflow**

## 📊 База данных

### Структура таблицы `index_metrics`:

```sql
CREATE TABLE index_metrics (
    id SERIAL PRIMARY KEY,
    index_code VARCHAR(10) NOT NULL,
    index_name VARCHAR(100) NOT NULL,
    index_value DECIMAL(5,2) NOT NULL,
    tokens_count INTEGER NOT NULL,
    raw_metrics JSONB NOT NULL,
    normalized_metrics JSONB NOT NULL,
    weights JSONB NOT NULL,
    tokens JSONB NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

### Пример SQL запросов:

```sql
-- Последние значения всех индексов
SELECT DISTINCT ON (index_code) 
    index_code, 
    index_name, 
    index_value, 
    timestamp
FROM index_metrics 
ORDER BY index_code, timestamp DESC;

-- История конкретного индекса за последние 24 часа
SELECT index_value, timestamp 
FROM index_metrics 
WHERE index_code = 'RWA' 
    AND timestamp > NOW() - INTERVAL '24 hours'
ORDER BY timestamp DESC;
```

## ⚙️ Конфигурация

### Изменение токенов в индексах:

Отредактируйте словарь `INDICES` в файле `main.py`:

```python
INDICES = {
    "RWA": {
        "name": "Real World Assets",
        "description": "Токены, связанные с реальными активами",
        "tokens": {
            46356: "ONDO",   # ID токена: символ
            # Добавьте новые токены здесь
        }
    }
}
```

### Изменение весов метрик:

```python
WEIGHTS = {
    'sentiment': 0.25,           # 25% вес
    'interactions_24h': 0.25,    # 25% вес
    'social_volume_24h': 0.25,   # 25% вес
    'alt_rank': 0.25             # 25% вес
}
```

### Изменение расписания запуска:

Отредактируйте файл `.github/workflows/data_collection.yml`:

```yaml
schedule:
  # Каждый час
  - cron: '0 * * * *'
  # Каждые 15 минут
  - cron: '*/15 * * * *'
  # Каждый день в 9:00 UTC
  - cron: '0 9 * * *'
```

## 🐛 Устранение неполадок

### Проблема: "Missing required environment variable"

**Решение**: Проверьте, что все secrets добавлены в GitHub и имеют правильные имена.

### Проблема: "Ошибка подключения к Supabase"

**Решение**: 
1. Проверьте правильность `SUPABASE_URL` и `SUPABASE_KEY`
2. Убедитесь, что таблица `index_metrics` создана в Supabase
3. Проверьте RLS (Row Level Security) настройки

### Проблема: "Не найдены токены"

**Решение**: 
1. Проверьте ID токенов в LunarCrush API
2. Возможно, токены временно недоступны
3. Проверьте логи для деталей

### Проблема: Workflow не запускается по расписанию

**Решение**:
1. Убедитесь, что в репозитории была активность в последние 60 дней
2. Проверьте синтаксис cron выражения
3. GitHub Actions может иметь задержки в несколько минут

## 📝 Логи

Система ведет подробные логи в файл `crypto_indices.log`. В случае ошибок в GitHub Actions, логи автоматически сохраняются как артефакты.

### Просмотр логов локально:

```bash
tail -f crypto_indices.log
```

## 🚨 Безопасность

- ❌ Никогда не коммитьте API ключи в код
- ✅ Используйте GitHub Secrets для чувствительных данных
- ✅ Регулярно ротируйте API ключи
- ✅ Ограничьте доступ к репозиторию только необходимым пользователям

## 📞 Поддержка

При возникновении проблем:

1. Проверьте логи выполнения
2. Убедитесь в правильности всех переменных окружения
3. Проверьте статус API LunarCrush
4. Проверьте подключение к Supabase

## 📄 Лицензия

MIT License

## 🤝 Вклад в проект

1. Форкните репозиторий
2. Создайте ветку для новой функции
3. Внесите изменения
4. Создайте Pull Request
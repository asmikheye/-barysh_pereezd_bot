# ARC RailGuard — Шаг 1: Статичный бот

## Быстрый старт

### 1. Установи Python (если нет)
Скачай с https://python.org — версия 3.10 или выше.

### 2. Создай виртуальное окружение
```bash
python -m venv venv
```

Активируй:
- Windows: `venv\Scripts\activate`
- Mac/Linux: `source venv/bin/activate`

### 3. Установи зависимости
```bash
pip install -r requirements.txt
```

### 4. Создай .env файл
Скопируй `.env.example` → `.env`
```bash
cp .env.example .env
```
Открой `.env` и вставь токен от @BotFather:
```
BOT_TOKEN=твой_токен_здесь
```

### 5. Запусти бота
```bash
python bot.py
```

## Проверка что всё работает

1. Открой Telegram, найди `@barysh_pereezd_bot`
2. Напиши `/start` — должно прийти сообщение с двумя переездами
3. Нажми `☕ Поддержать` — должен прийти номер СБП
4. Напиши `/status` — то же самое что /start

## Структура файлов

```
railguard/
├── bot.py           ← главный файл бота
├── .env             ← твой токен (не публиковать!)
├── .env.example     ← шаблон для .env
├── requirements.txt ← зависимости Python
└── README.md        ← эта инструкция
```

## Что дальше — Шаг 2

После того как Шаг 1 работает стабильно — добавляем живой таймер:
сообщение редактируется каждую минуту, таймер тикает сам по себе.

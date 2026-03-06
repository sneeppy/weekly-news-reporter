# weekly-news-reporter

В папке лежит:

- `answer.md` — текстовый разбор задания (инструменты, автоматизация, анти‑галлюцинации) + готовый промт.
- `prototype/` — простой рабочий прототип без API‑ключей:
  - `prototype/news_reporter.py` — сбор RSS/Atom, сохранение в SQLite, недельный отчёт в Markdown.
  - `prototype/config.example.json` — пример конфигурации (ленты + темы/ключевые слова).
  - `prototype/run_weekly.ps1` — пример запуска (можно повесить на Планировщик заданий Windows).

## Быстрый старт (Windows)

1) Скопируйте конфиг:

```powershell
cd "c:\Users\Павел\Desktop\test\prototype"
copy .\config.example.json .\config.json
```

2) Запустите сбор и генерацию отчёта:

```powershell
python .\news_reporter.py --config .\config.json
```

3) Результат:

- SQLite база: `prototype\data\news.db`
- Отчёты: `prototype\reports\report_YYYY-MM-DD.md`

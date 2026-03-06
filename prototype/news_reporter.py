import argparse
import datetime as dt
import html
import json
import os
import re
import sqlite3
import textwrap
import urllib.request
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from email.utils import parsedate_to_datetime


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _fetch(url: str, *, timeout_seconds: int, user_agent: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
        return resp.read()


_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(s: str) -> str:
    s = s or ""
    # Decode HTML entities first (&nbsp; etc.)
    s = html.unescape(s)
    s = s.replace("\u00a0", " ")
    s = _TAG_RE.sub(" ", s)
    s = re.sub(r"\bчитать\s+далее\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    return s.strip()


def _safe_text(elem: ET.Element | None) -> str:
    if elem is None:
        return ""
    return _strip_html("".join(elem.itertext()))


def _parse_date(s: str) -> dt.datetime | None:
    if not s:
        return None
    s = s.strip()
    try:
        d = parsedate_to_datetime(s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d.astimezone(dt.timezone.utc)
    except Exception:
        pass

    # Atom ISO timestamps (best effort)
    try:
        s2 = s.replace("Z", "+00:00")
        d = dt.datetime.fromisoformat(s2)
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d.astimezone(dt.timezone.utc)
    except Exception:
        return None


def _as_local(d: dt.datetime) -> dt.datetime:
    # Convert UTC -> local time (system tz)
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.astimezone()


def parse_rss_or_atom(xml_bytes: bytes, *, default_source: str) -> list[dict]:
    root = ET.fromstring(xml_bytes)

    # Handle namespaces by stripping them for easier access
    for elem in root.iter():
        if "}" in elem.tag:
            elem.tag = elem.tag.split("}", 1)[1]

    items: list[dict] = []

    if root.tag.lower() == "rss":
        channel = root.find("channel")
        if channel is None:
            return items

        source_title = _safe_text(channel.find("title")) or default_source

        for it in channel.findall("item"):
            title = _safe_text(it.find("title"))
            link = _safe_text(it.find("link"))
            guid = _safe_text(it.find("guid")) or link
            pub = _parse_date(_safe_text(it.find("pubDate")))
            desc = _safe_text(it.find("description"))
            if not title or not link:
                continue
            items.append(
                {
                    "guid": guid,
                    "title": title,
                    "link": link,
                    "published_at_utc": pub,
                    "source": source_title,
                    "snippet": desc,
                }
            )
        return items

    # Atom
    if root.tag.lower() == "feed":
        source_title = _safe_text(root.find("title")) or default_source

        for entry in root.findall("entry"):
            title = _safe_text(entry.find("title"))

            link = ""
            for link_el in entry.findall("link"):
                rel = (link_el.attrib.get("rel") or "").lower()
                href = link_el.attrib.get("href") or ""
                if not href:
                    continue
                if rel in ("", "alternate"):
                    link = href
                    break
            if not link:
                # fallback: first href
                for link_el in entry.findall("link"):
                    href = link_el.attrib.get("href") or ""
                    if href:
                        link = href
                        break

            guid = _safe_text(entry.find("id")) or link
            published = _parse_date(_safe_text(entry.find("published"))) or _parse_date(
                _safe_text(entry.find("updated"))
            )
            summary = _safe_text(entry.find("summary"))
            content = _safe_text(entry.find("content"))
            snippet = summary or content

            if not title or not link:
                continue
            items.append(
                {
                    "guid": guid,
                    "title": title,
                    "link": link,
                    "published_at_utc": published,
                    "source": source_title,
                    "snippet": snippet,
                }
            )
        return items

    return items


def init_db(db_path: str) -> None:
    _ensure_dir(os.path.dirname(db_path))
    with sqlite3.connect(db_path) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS items (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              guid TEXT,
              title TEXT NOT NULL,
              link TEXT NOT NULL UNIQUE,
              source TEXT NOT NULL,
              published_at_utc TEXT,
              snippet TEXT,
              inserted_at_utc TEXT NOT NULL
            );
            """
        )
        con.execute("CREATE INDEX IF NOT EXISTS idx_items_published ON items(published_at_utc);")


def upsert_items(db_path: str, items: list[dict]) -> int:
    inserted = 0
    now = _utcnow().isoformat()
    with sqlite3.connect(db_path) as con:
        for it in items:
            pub = it.get("published_at_utc")
            pub_iso = pub.isoformat() if isinstance(pub, dt.datetime) else None
            try:
                cur = con.execute(
                    """
                    INSERT OR IGNORE INTO items(guid,title,link,source,published_at_utc,snippet,inserted_at_utc)
                    VALUES(?,?,?,?,?,?,?)
                    """,
                    (
                        it.get("guid") or it.get("link"),
                        it["title"],
                        it["link"],
                        it.get("source") or "unknown",
                        pub_iso,
                        (it.get("snippet") or "")[:4000],
                        now,
                    ),
                )
                if cur.rowcount == 1:
                    inserted += 1
            except sqlite3.Error:
                # Ignore malformed rows; keep pipeline resilient
                continue
    return inserted


def _normalize_for_match(s: str) -> str:
    s = (s or "").lower()
    s = s.replace("ё", "е")
    return s


def assign_topics(items: list[dict], topics: list[dict]) -> dict[str, list[dict]]:
    buckets: dict[str, list[dict]] = defaultdict(list)

    compiled = []
    for t in topics:
        kws = [kw.strip().lower() for kw in (t.get("keywords") or []) if kw.strip()]
        if not kws:
            continue
        # basic substring match; transparent and stable
        compiled.append((t["name"], kws))

    for it in items:
        text = _normalize_for_match(f"{it.get('title','')} {it.get('snippet','')}")
        matched = False
        for name, kws in compiled:
            if any(kw in text for kw in kws):
                buckets[name].append(it)
                matched = True
        if not matched:
            buckets["Другое"].append(it)
    return buckets


_RU_STOPWORDS = {
    "и",
    "в",
    "во",
    "на",
    "с",
    "со",
    "к",
    "ко",
    "по",
    "за",
    "из",
    "у",
    "о",
    "об",
    "от",
    "для",
    "это",
    "как",
    "что",
    "не",
    "но",
    "а",
    "или",
    "при",
    "мы",
    "вы",
    "они",
    "он",
    "она",
    "оно",
    "так",
    "же",
    "то",
    "до",
    "после",
    "без",
    "над",
    "под",
    "про",
    "также",
    "еще",
    "ещё",
    "этот",
    "эта",
    "эти",
    "все",
    "всё",
    "их",
    "его",
    "ее",
    "её",
}


def top_terms(items: list[dict], *, n: int = 20) -> list[tuple[str, int]]:
    text = " ".join([_strip_html(it.get("title", "")) + " " + _strip_html(it.get("snippet", "")) for it in items])
    text = _normalize_for_match(text)
    # keep latin words (AI/LLM) and cyrillic, drop numbers
    words = re.findall(r"[a-z]{2,}|[а-я]{3,}", text, flags=re.IGNORECASE)
    words = [w for w in words if w not in _RU_STOPWORDS]
    return Counter(words).most_common(n)


def load_items_for_period(db_path: str, *, since_utc: dt.datetime) -> list[dict]:
    since_iso = since_utc.isoformat()
    with sqlite3.connect(db_path) as con:
        rows = con.execute(
            """
            SELECT title, link, source, published_at_utc, snippet
            FROM items
            WHERE published_at_utc IS NOT NULL AND published_at_utc >= ?
            ORDER BY published_at_utc DESC
            """,
            (since_iso,),
        ).fetchall()

    out: list[dict] = []
    for title, link, source, published_at_utc, snippet in rows:
        pub = None
        try:
            pub = dt.datetime.fromisoformat(published_at_utc)
            if pub.tzinfo is None:
                pub = pub.replace(tzinfo=dt.timezone.utc)
        except Exception:
            pub = None
        out.append(
            {
                "title": title,
                "link": link,
                "source": source,
                "published_at_utc": pub,
                "snippet": snippet or "",
            }
        )
    return out


def write_report(
    path: str,
    *,
    period_days: int,
    items: list[dict],
    topics: list[dict],
    max_items_per_topic: int,
) -> None:
    _ensure_dir(os.path.dirname(path))

    now_local = dt.datetime.now().astimezone()
    since_utc = (_utcnow() - dt.timedelta(days=period_days)).replace(microsecond=0)

    # analytics
    by_source = Counter([it.get("source") or "unknown" for it in items])
    by_day = Counter()
    for it in items:
        pub = it.get("published_at_utc")
        if isinstance(pub, dt.datetime):
            by_day[_as_local(pub).date().isoformat()] += 1

    buckets = assign_topics(items, topics)

    with open(path, "w", encoding="utf-8") as f:
        f.write(f"# Недельный отчёт по новостям\n\n")
        f.write(f"- Сформирован: **{now_local.strftime('%Y-%m-%d %H:%M %Z')}**\n")
        f.write(f"- Период: последние **{period_days}** дней (с {since_utc.date().isoformat()})\n")
        f.write(f"- Всего новостей: **{len(items)}**\n\n")

        f.write("## Краткая аналитика\n\n")
        if by_day:
            f.write("### Динамика по дням\n\n")
            for day, cnt in sorted(by_day.items()):
                f.write(f"- **{day}**: {cnt}\n")
            f.write("\n")

        if by_source:
            f.write("### Топ источников\n\n")
            for src, cnt in by_source.most_common(10):
                f.write(f"- **{src}**: {cnt}\n")
            f.write("\n")

        terms = top_terms(items, n=20)
        if terms:
            f.write("### Топ термины недели (по заголовкам/сниппетам)\n\n")
            f.write(", ".join([f"`{w}` ({c})" for w, c in terms]) + "\n\n")

        f.write("## Подборка по темам\n\n")
        # stable order: configured topics first, then "Другое"
        ordered_topic_names = [t["name"] for t in topics if t.get("name")] + ["Другое"]
        seen = set()
        for name in ordered_topic_names:
            if name in seen:
                continue
            seen.add(name)
            lst = buckets.get(name, [])
            f.write(f"### {name}\n\n")
            if not lst:
                f.write("_Нет релевантных новостей за период._\n\n")
                continue

            for it in lst[:max_items_per_topic]:
                pub = it.get("published_at_utc")
                pub_str = _as_local(pub).strftime("%Y-%m-%d") if isinstance(pub, dt.datetime) else "n/a"
                snippet = _strip_html(it.get("snippet", ""))
                snippet = textwrap.shorten(snippet, width=220, placeholder="…")
                f.write(f"- **{it.get('title','').strip()}**\n")
                f.write(f"  - Дата: {pub_str}\n")
                f.write(f"  - Источник: {it.get('source','')}\n")
                f.write(f"  - Ссылка: {it.get('link','')}\n")
                if snippet:
                    f.write(f"  - Коротко: {snippet}\n")
            f.write("\n")


def main() -> int:
    ap = argparse.ArgumentParser(description="Weekly news reporter (RSS/Atom) with no hallucinations.")
    ap.add_argument("--config", required=True, help="Path to config.json")
    ap.add_argument("--no-fetch", action="store_true", help="Skip fetching, only build report from DB")
    args = ap.parse_args()

    cfg = _load_json(args.config)
    feeds = cfg.get("feeds") or []
    topics = cfg.get("topics") or []
    report_cfg = cfg.get("report") or {}
    http_cfg = cfg.get("http") or {}

    period_days = int(report_cfg.get("days") or 7)
    max_items_per_topic = int(report_cfg.get("max_items_per_topic") or 12)
    output_dir = report_cfg.get("output_dir") or "reports"
    data_dir = report_cfg.get("data_dir") or "data"

    timeout_seconds = int(http_cfg.get("timeout_seconds") or 20)
    user_agent = http_cfg.get("user_agent") or "weekly-news-reporter/1.0"

    base_dir = os.path.dirname(os.path.abspath(args.config))
    output_dir = os.path.join(base_dir, output_dir)
    data_dir = os.path.join(base_dir, data_dir)
    db_path = os.path.join(data_dir, "news.db")

    init_db(db_path)

    if not args.no_fetch:
        fetched_total = 0
        inserted_total = 0
        for feed in feeds:
            name = feed.get("name") or "feed"
            url = feed.get("url") or ""
            if not url:
                continue
            try:
                xml_bytes = _fetch(url, timeout_seconds=timeout_seconds, user_agent=user_agent)
                parsed = parse_rss_or_atom(xml_bytes, default_source=name)
                fetched_total += len(parsed)
                inserted_total += upsert_items(db_path, parsed)
            except Exception:
                # keep going if one feed fails
                continue

        print(f"Fetched items: {fetched_total}")
        print(f"Inserted new items: {inserted_total}")

    since_utc = (_utcnow() - dt.timedelta(days=period_days)).replace(microsecond=0)
    items = load_items_for_period(db_path, since_utc=since_utc)

    report_name = f"report_{dt.datetime.now().astimezone().date().isoformat()}.md"
    report_path = os.path.join(output_dir, report_name)
    write_report(
        report_path,
        period_days=period_days,
        items=items,
        topics=topics,
        max_items_per_topic=max_items_per_topic,
    )
    print(f"Report written: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


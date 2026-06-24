#!/usr/bin/env python3
"""
Read-only Polymarket event/market exporter.

Accepts a Polymarket event URL, event/market slug, or search phrase.
Uses only public GET endpoints:
  - Gamma API for event/market discovery and metadata
  - CLOB API for order-book snapshots

Outputs:
  - <output_prefix>_event.csv
  - <output_prefix>_markets.csv
  - <output_prefix>_prices.csv
  - <output_prefix>_orderbook.csv
  - <output_prefix>.xlsx

Install:
  pip install requests pandas xlsxwriter

Examples:
  python polymarket_readonly_exporter.py "https://polymarket.com/event/2026-fifa-world-cup-winner-595"
  python polymarket_readonly_exporter.py "2026-fifa-world-cup-winner-595" --output world_cup
  python polymarket_readonly_exporter.py "EPL which clubs get relegated" --output epl_relegation
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote, urlparse

import pandas as pd
import requests

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
USER_AGENT = "polymarket-readonly-exporter/1.0"


class PolymarketLookupError(RuntimeError):
    pass


@dataclass
class ApiClient:
    timeout: int = 20
    pause_seconds: float = 0.05

    def get(self, url: str, params: dict[str, Any] | None = None) -> Any:
        headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
        response = requests.get(url, params=params, headers=headers, timeout=self.timeout)
        if self.pause_seconds:
            time.sleep(self.pause_seconds)
        response.raise_for_status()
        if not response.text.strip():
            return None
        return response.json()


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def safe_filename(text: str, fallback: str = "polymarket_export") -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", text).strip("_")
    return cleaned[:120] or fallback


def parse_possible_slug(value: str) -> tuple[str, str]:
    """
    Returns (kind, text), where kind is one of: event_slug, market_slug, slug, search.
    """
    raw = value.strip()
    parsed = urlparse(raw)
    if parsed.scheme and parsed.netloc:
        parts = [p for p in parsed.path.split("/") if p]
        for marker in ("event", "market", "markets"):
            if marker in parts:
                idx = parts.index(marker)
                if idx + 1 < len(parts):
                    kind = "event_slug" if marker == "event" else "market_slug"
                    return kind, parts[idx + 1]
        # Last path segment is often still a slug.
        if parts:
            return "slug", parts[-1]
        raise PolymarketLookupError(f"Could not find a slug in URL: {value}")

    # Slug-like input: no spaces and looks URL-safe.
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", raw) and ("-" in raw or "_" in raw):
        return "slug", raw

    return "search", raw


def as_list(payload: Any) -> list[Any]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        # Common wrappers used by search-style endpoints.
        for key in ("events", "markets", "data", "results", "items"):
            if isinstance(payload.get(key), list):
                return payload[key]
        return [payload]
    return []


def parse_jsonish(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (list, dict, int, float, bool)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return value
    return value


def first_present(d: dict[str, Any], keys: Iterable[str], default: Any = None) -> Any:
    for key in keys:
        if key in d and d[key] not in (None, ""):
            return d[key]
    return default


def gamma_event_by_slug(client: ApiClient, slug: str) -> dict[str, Any] | None:
    attempts = [
        (f"{GAMMA_BASE}/events/slug/{quote(slug)}", None),
        (f"{GAMMA_BASE}/events", {"slug": slug}),
    ]
    for url, params in attempts:
        try:
            payload = client.get(url, params=params)
            items = as_list(payload)
            if items:
                return items[0]
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code in (400, 404):
                continue
            raise
    return None


def gamma_market_by_slug(client: ApiClient, slug: str) -> dict[str, Any] | None:
    attempts = [
        (f"{GAMMA_BASE}/markets/slug/{quote(slug)}", None),
        (f"{GAMMA_BASE}/markets", {"slug": slug}),
    ]
    for url, params in attempts:
        try:
            payload = client.get(url, params=params)
            items = as_list(payload)
            if items:
                return items[0]
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code in (400, 404):
                continue
            raise
    return None


def search_events(client: ApiClient, phrase: str, limit: int = 10, include_closed: bool = False) -> list[dict[str, Any]]:
    """
    Search is intentionally defensive because Gamma search response shapes have changed over time.
    It tries public-search first, then events/markets keyword-style fallbacks.
    """
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add_event(item: dict[str, Any]) -> None:
        slug = str(first_present(item, ("slug", "eventSlug", "ticker", "id"), ""))
        key = slug or json.dumps(item, sort_keys=True)[:300]
        if key not in seen:
            seen.add(key)
            candidates.append(item)

    search_attempts = [
        (f"{GAMMA_BASE}/public-search", {"q": phrase, "limit": limit}),
        (f"{GAMMA_BASE}/public-search", {"query": phrase, "limit": limit}),
        (f"{GAMMA_BASE}/events", {"q": phrase, "limit": limit, "closed": str(include_closed).lower()}),
        (f"{GAMMA_BASE}/events", {"search": phrase, "limit": limit, "closed": str(include_closed).lower()}),
        (f"{GAMMA_BASE}/markets", {"q": phrase, "limit": limit, "closed": str(include_closed).lower()}),
        (f"{GAMMA_BASE}/markets", {"search": phrase, "limit": limit, "closed": str(include_closed).lower()}),
    ]

    for url, params in search_attempts:
        try:
            payload = client.get(url, params=params)
        except requests.HTTPError:
            continue

        if isinstance(payload, dict):
            for key in ("events", "eventResults", "results"):
                for item in payload.get(key, []) or []:
                    if isinstance(item, dict):
                        # Some public search rows wrap the event under an event key.
                        add_event(item.get("event") if isinstance(item.get("event"), dict) else item)
            for item in payload.get("markets", []) or []:
                if isinstance(item, dict):
                    event = event_from_market(client, item)
                    if event:
                        add_event(event)
        else:
            for item in as_list(payload):
                if isinstance(item, dict):
                    if "markets" in item or "series" in item or "closed" in item or "active" in item:
                        add_event(item)
                    else:
                        event = event_from_market(client, item)
                        if event:
                            add_event(event)

        if candidates:
            break

    return candidates[:limit]


def event_from_market(client: ApiClient, market: dict[str, Any]) -> dict[str, Any] | None:
    # If a market row embeds event data, use it directly.
    embedded = market.get("event") or market.get("events")
    if isinstance(embedded, dict):
        return embedded
    if isinstance(embedded, list) and embedded and isinstance(embedded[0], dict):
        return embedded[0]

    # Otherwise try event slug/id fields if present.
    event_slug = first_present(market, ("eventSlug", "event_slug", "eventTicker", "event_ticker"))
    if event_slug:
        found = gamma_event_by_slug(client, str(event_slug))
        if found:
            return found

    # Last resort: make a synthetic single-market event.
    return {
        "id": market.get("eventId") or market.get("id"),
        "slug": market.get("eventSlug") or market.get("slug"),
        "title": market.get("eventTitle") or market.get("question") or market.get("title"),
        "markets": [market],
        "_synthetic_from_market": True,
    }


def resolve_event(client: ApiClient, user_input: str, limit: int, include_closed: bool) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    kind, text = parse_possible_slug(user_input)

    if kind in ("event_slug", "slug"):
        event = gamma_event_by_slug(client, text)
        if event:
            return event, [event]

    if kind in ("market_slug", "slug"):
        market = gamma_market_by_slug(client, text)
        if market:
            event = event_from_market(client, market)
            if event:
                # Ensure the matched market is present if the event lookup was not rich.
                if not event.get("markets"):
                    event["markets"] = [market]
                return event, [event]

    results = search_events(client, text, limit=limit, include_closed=include_closed)
    if not results:
        raise PolymarketLookupError(f"No matching Polymarket event/market found for: {user_input}")
    return results[0], results


def extract_markets(event: dict[str, Any]) -> list[dict[str, Any]]:
    markets = event.get("markets")
    if isinstance(markets, list):
        return [m for m in markets if isinstance(m, dict)]
    if isinstance(markets, dict):
        return [markets]
    # Single market fallback.
    if any(k in event for k in ("conditionId", "clobTokenIds", "question")):
        return [event]
    return []


def extract_tokens(market: dict[str, Any]) -> list[dict[str, Any]]:
    """Return token rows with outcome name and CLOB token_id."""
    rows: list[dict[str, Any]] = []

    tokens = parse_jsonish(market.get("tokens"))
    if isinstance(tokens, list):
        for token in tokens:
            if not isinstance(token, dict):
                continue
            token_id = first_present(token, ("token_id", "tokenId", "asset_id", "assetId", "id"))
            if token_id:
                rows.append({
                    "outcome": first_present(token, ("outcome", "name", "value"), ""),
                    "token_id": str(token_id),
                })

    outcomes = parse_jsonish(market.get("outcomes"))
    clob_token_ids = parse_jsonish(first_present(market, ("clobTokenIds", "clob_token_ids", "clobTokenIDs")))

    if isinstance(outcomes, str):
        outcomes = [outcomes]
    if isinstance(clob_token_ids, str):
        clob_token_ids = [clob_token_ids]

    if isinstance(outcomes, list) and isinstance(clob_token_ids, list):
        for outcome, token_id in zip(outcomes, clob_token_ids):
            if token_id and not any(r["token_id"] == str(token_id) for r in rows):
                rows.append({"outcome": str(outcome), "token_id": str(token_id)})

    return rows


def fetch_book(client: ApiClient, token_id: str) -> dict[str, Any] | None:
    try:
        return client.get(f"{CLOB_BASE}/book", params={"token_id": token_id})
    except requests.HTTPError:
        # Quietly skip tokens that Gamma lists but CLOB does not currently expose.
        return None


def to_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def summarise_book(book: dict[str, Any] | None) -> dict[str, Any]:
    if not book:
        return {
            "best_bid": None, "best_ask": None, "spread": None, "midpoint": None,
            "last_trade_price": None, "bid_levels": 0, "ask_levels": 0,
            "total_bid_size": None, "total_ask_size": None,
        }

    bids = book.get("bids") or []
    asks = book.get("asks") or []
    bid_prices = [to_float(x.get("price")) for x in bids if isinstance(x, dict)]
    ask_prices = [to_float(x.get("price")) for x in asks if isinstance(x, dict)]
    bid_prices = [x for x in bid_prices if x is not None]
    ask_prices = [x for x in ask_prices if x is not None]

    best_bid = max(bid_prices) if bid_prices else None
    best_ask = min(ask_prices) if ask_prices else None
    spread = (best_ask - best_bid) if best_bid is not None and best_ask is not None else None
    midpoint = ((best_bid + best_ask) / 2) if best_bid is not None and best_ask is not None else None

    def total_size(levels: list[Any]) -> float | None:
        vals = [to_float(x.get("size")) for x in levels if isinstance(x, dict)]
        vals = [x for x in vals if x is not None]
        return sum(vals) if vals else None

    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "midpoint": midpoint,
        "last_trade_price": to_float(book.get("last_trade_price")),
        "bid_levels": len(bids),
        "ask_levels": len(asks),
        "total_bid_size": total_size(bids),
        "total_ask_size": total_size(asks),
        "min_order_size": to_float(book.get("min_order_size")),
        "tick_size": to_float(book.get("tick_size")),
        "neg_risk": book.get("neg_risk"),
        "book_timestamp": book.get("timestamp"),
        "book_hash": book.get("hash"),
    }


def build_rows(
    client: ApiClient,
    event: dict[str, Any],
    top_n_book: int,
    outcome_filter: set[str] | None = None,
    max_markets: int | None = None,
    parallel_workers: int = 16,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build event/market/price/order-book tables.

    Live-view optimisation:
      - can limit markets before fetching books, so we do not fetch teams that will not be displayed
      - fetches token order books in parallel instead of one-by-one
    """
    fetched_at = now_utc_iso()
    event_id = first_present(event, ("id", "eventId"))
    event_slug = first_present(event, ("slug", "ticker"))
    event_title = first_present(event, ("title", "name", "question"))

    event_df = pd.DataFrame([{
        "fetched_at_utc": fetched_at,
        "event_id": event_id,
        "event_slug": event_slug,
        "event_title": event_title,
        "event_url": f"https://polymarket.com/event/{event_slug}" if event_slug else None,
        "active": event.get("active"),
        "closed": event.get("closed"),
        "archived": event.get("archived"),
        "startDate": event.get("startDate") or event.get("start_date"),
        "endDate": event.get("endDate") or event.get("end_date"),
        "volume": event.get("volume"),
        "volume24hr": event.get("volume24hr") or event.get("volume_24hr"),
        "liquidity": event.get("liquidity"),
        "raw_event_json": json.dumps(event, ensure_ascii=False, default=str)[:32700],
    }])

    markets = list(extract_markets(event))
    if max_markets is not None and max_markets > 0:
        markets = markets[:max_markets]

    market_rows: list[dict[str, Any]] = []
    tasks: list[dict[str, Any]] = []

    for market_index, market in enumerate(markets, start=1):
        market_id = first_present(market, ("id", "marketId"))
        market_slug = first_present(market, ("slug", "ticker"))
        question = first_present(market, ("question", "title", "name"))
        condition_id = first_present(market, ("conditionId", "condition_id"))
        enable_order_book = first_present(market, ("enableOrderBook", "enable_order_book"))
        outcomes = parse_jsonish(market.get("outcomes"))
        token_rows = extract_tokens(market)

        market_rows.append({
            "fetched_at_utc": fetched_at,
            "event_id": event_id,
            "event_slug": event_slug,
            "market_index": market_index,
            "market_id": market_id,
            "market_slug": market_slug,
            "question": question,
            "condition_id": condition_id,
            "enable_order_book": enable_order_book,
            "active": market.get("active"),
            "closed": market.get("closed"),
            "archived": market.get("archived"),
            "volume": market.get("volume"),
            "volume24hr": market.get("volume24hr") or market.get("volume_24hr"),
            "liquidity": market.get("liquidity"),
            "outcomes": json.dumps(outcomes, ensure_ascii=False, default=str),
            "clob_token_count": len(token_rows),
            "raw_market_json": json.dumps(market, ensure_ascii=False, default=str)[:32700],
        })

        for token in token_rows:
            if outcome_filter is not None and normalise_outcome(token.get("outcome")) not in outcome_filter:
                continue
            tasks.append({
                "event_id": event_id,
                "event_slug": event_slug,
                "market_index": market_index,
                "market_id": market_id,
                "market_slug": market_slug,
                "question": question,
                "condition_id": condition_id,
                "outcome": token.get("outcome"),
                "token_id": token["token_id"],
            })

    price_rows: list[dict[str, Any]] = []
    book_rows: list[dict[str, Any]] = []

    def fetch_task(task: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any] | None]:
        return task, fetch_book(client, task["token_id"])

    if tasks:
        workers = max(1, min(parallel_workers, len(tasks)))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(fetch_task, task) for task in tasks]
            for future in as_completed(futures):
                task, book = future.result()
                summary = summarise_book(book)
                price_rows.append({
                    "fetched_at_utc": fetched_at,
                    **task,
                    **summary,
                })

                if book:
                    for side in ("bids", "asks"):
                        levels = book.get(side) or []
                        sorted_levels = sorted(
                            [x for x in levels if isinstance(x, dict)],
                            key=lambda x: to_float(x.get("price")) or 0.0,
                            reverse=(side == "bids"),
                        )
                        for level_no, level in enumerate(sorted_levels[:top_n_book], start=1):
                            book_rows.append({
                                "fetched_at_utc": fetched_at,
                                "event_id": event_id,
                                "event_slug": event_slug,
                                "market_index": task["market_index"],
                                "market_id": task["market_id"],
                                "market_slug": task["market_slug"],
                                "question": task["question"],
                                "outcome": task["outcome"],
                                "token_id": task["token_id"],
                                "side": side[:-1],
                                "level": level_no,
                                "price": to_float(level.get("price")),
                                "size": to_float(level.get("size")),
                                "raw_level_json": json.dumps(level, ensure_ascii=False, default=str),
                            })

    return (
        event_df,
        pd.DataFrame(market_rows),
        pd.DataFrame(price_rows),
        pd.DataFrame(book_rows),
    )



def clean_team_name(question: Any) -> str:
    """Best-effort cleaner for outright winner binary market questions."""
    text = str(question or "").strip()
    text = re.sub(r"\s+", " ", text)
    patterns = [
        r"^Will\s+(.+?)\s+win\s+the\s+.+?\??$",
        r"^Will\s+(.+?)\s+win\s+.+?\??$",
        r"^(.+?)\s+to\s+win\s+.+?\??$",
        r"^(.+?)\s+winner\??$",
    ]
    for pattern in patterns:
        m = re.match(pattern, text, flags=re.I)
        if m:
            return m.group(1).strip(" ?:-")
    # Polymarket outright questions often include just the team in title/name fields.
    return text.rstrip("?")


def normalise_outcome(value: Any) -> str:
    return str(value or "").strip().lower()


def pick_market_field(markets_df: pd.DataFrame, market_index: int, field: str) -> Any:
    if markets_df.empty or field not in markets_df.columns:
        return None
    match = markets_df.loc[markets_df["market_index"] == market_index]
    if match.empty:
        return None
    return match.iloc[0].get(field)


def best_levels(book_df: pd.DataFrame, market_index: int, outcome: str, side: str, n: int = 3) -> list[tuple[float | None, float | None]]:
    """Return [(price, size)] for top n levels. Bids high-to-low; asks low-to-high."""
    if book_df.empty:
        return []
    df = book_df[
        (book_df.get("market_index") == market_index)
        & (book_df.get("outcome").astype(str).str.lower() == outcome.lower())
        & (book_df.get("side") == side)
    ].copy()
    if df.empty:
        return []
    ascending = side == "ask"
    df = df.sort_values("price", ascending=ascending).head(n)
    return [(to_float(r.get("price")), to_float(r.get("size"))) for _, r in df.iterrows()]


def price_summary(prices_df: pd.DataFrame, market_index: int, outcome: str) -> dict[str, Any]:
    if prices_df.empty:
        return {}
    df = prices_df[
        (prices_df.get("market_index") == market_index)
        & (prices_df.get("outcome").astype(str).str.lower() == outcome.lower())
    ]
    if df.empty:
        return {}
    return df.iloc[0].to_dict()


def build_team_summary(markets_df: pd.DataFrame, prices_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if markets_df.empty:
        return pd.DataFrame()
    for _, market in markets_df.iterrows():
        idx = int(market["market_index"])
        question = market.get("question")
        yes = price_summary(prices_df, idx, "yes")
        no = price_summary(prices_df, idx, "no")
        rows.append({
            "team": clean_team_name(question),
            "question": question,
            "market_index": idx,
            "market_id": market.get("market_id"),
            "market_slug": market.get("market_slug"),
            "active": market.get("active"),
            "closed": market.get("closed"),
            "volume": to_float(market.get("volume")),
            "liquidity": to_float(market.get("liquidity")),
            "yes_bid": yes.get("best_bid"),
            "yes_ask": yes.get("best_ask"),
            "yes_mid": yes.get("midpoint"),
            "yes_last": yes.get("last_trade_price"),
            "yes_bid_levels": yes.get("bid_levels"),
            "yes_ask_levels": yes.get("ask_levels"),
            "no_bid": no.get("best_bid"),
            "no_ask": no.get("best_ask"),
            "no_mid": no.get("midpoint"),
            "no_last": no.get("last_trade_price"),
            "no_bid_levels": no.get("bid_levels"),
            "no_ask_levels": no.get("ask_levels"),
        })
    df = pd.DataFrame(rows)
    if not df.empty and "yes_mid" in df.columns:
        df = df.sort_values(["yes_mid", "yes_bid", "volume"], ascending=[False, False, False], na_position="last")
    return df


def build_ladder(markets_df: pd.DataFrame, prices_df: pd.DataFrame, book_df: pd.DataFrame, outcome: str = "Yes", levels: int = 3) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if markets_df.empty:
        return pd.DataFrame()
    outcome_l = outcome.lower()
    for _, market in markets_df.iterrows():
        idx = int(market["market_index"])
        question = market.get("question")
        bids = best_levels(book_df, idx, outcome_l, "bid", levels)
        asks = best_levels(book_df, idx, outcome_l, "ask", levels)
        ps = price_summary(prices_df, idx, outcome_l)

        # If book levels are missing, use the summary best bid/ask as level 1 fallback.
        if not bids and ps.get("best_bid") is not None:
            bids = [(ps.get("best_bid"), ps.get("total_bid_size"))]
        if not asks and ps.get("best_ask") is not None:
            asks = [(ps.get("best_ask"), ps.get("total_ask_size"))]

        row: dict[str, Any] = {
            "team": clean_team_name(question),
            "question": question,
            "market_index": idx,
            "market_slug": market.get("market_slug"),
            "volume": to_float(market.get("volume")),
            "liquidity": to_float(market.get("liquidity")),
        }

        # Betfair-style trading view for a Yes outcome:
        # - Back = prices available to buy/back Yes, so use the Yes asks.
        # - Lay = prices available to sell/lay Yes, so use the Yes bids.
        # Back/Lay 1 is the best price nearest the centre of the ladder.
        ordered_back = asks[:levels]
        while len(ordered_back) < levels:
            ordered_back.append((None, None))
        for i, (price, size) in enumerate(ordered_back, start=1):
            row[f"back_{i}"] = price
            row[f"back_{i}_size"] = size

        ordered_lay = bids[:levels]
        while len(ordered_lay) < levels:
            ordered_lay.append((None, None))
        for i, (price, size) in enumerate(ordered_lay, start=1):
            row[f"lay_{i}"] = price
            row[f"lay_{i}_size"] = size

        row["last"] = ps.get("last_trade_price")
        row["mid"] = ps.get("midpoint")
        row["spread"] = ps.get("spread")
        rows.append(row)

    df = pd.DataFrame(rows)
    if not df.empty:
        sort_col = "mid" if "mid" in df.columns else "back_3"
        df = df.sort_values([sort_col, "volume"], ascending=[False, False], na_position="last")
    return df


def prob_to_cents(value: Any) -> float | None:
    v = to_float(value)
    if v is None:
        return None
    return v * 100.0


def prob_to_decimal(value: Any) -> float | None:
    v = to_float(value)
    if v is None or v <= 0:
        return None
    return 1.0 / v


def build_betfair_view(yes_ladder_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean Betfair-style trading view for the Yes side.

    Price columns are displayed as cents per share: 0.163 -> 16.3.
    Decimal columns convert probability price to decimal odds: 0.163 -> 6.135.
    Volume and liquidity are deliberately omitted from this display view.
    """
    if yes_ladder_df.empty:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for _, row in yes_ladder_df.iterrows():
        out: dict[str, Any] = {"team": row.get("team")}

        # Back 3/2/1 are Polymarket Yes bids, with Back 1 nearest the current price.
        for level in (3, 2, 1):
            out[f"back_{level}"] = prob_to_cents(row.get(f"back_{level}"))
            out[f"back_{level}_size"] = to_float(row.get(f"back_{level}_size"))

        # Lay 1/2/3 are Polymarket Yes asks, with Lay 1 nearest the current price.
        for level in (1, 2, 3):
            out[f"lay_{level}"] = prob_to_cents(row.get(f"lay_{level}"))
            out[f"lay_{level}_size"] = to_float(row.get(f"lay_{level}_size"))

        out["back_1_decimal"] = prob_to_decimal(row.get("back_1"))
        out["lay_1_decimal"] = prob_to_decimal(row.get("lay_1"))
        rows.append(out)

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["back_1"], ascending=[False], na_position="last")
    return df



# -----------------------------
# Streamlit live viewer
# -----------------------------
import html

import streamlit as st
from streamlit_autorefresh import st_autorefresh

DEFAULT_EVENT = "https://polymarket.com/event/2026-fifa-world-cup-winner-595"
LOCKED_EVENT_SLUG = "2026-fifa-world-cup-winner-595"
LOCKED_EVENT_TITLE = "World Cup Winner"


def event_market_count(event: dict[str, Any]) -> int:
    markets = event.get("markets") or []
    return len(markets) if isinstance(markets, list) else 0


def event_matches_locked_world_cup(event: dict[str, Any] | None) -> bool:
    if not isinstance(event, dict):
        return False
    title = str(first_present(event, ("title", "name", "question"), "")).strip().lower()
    slug = str(first_present(event, ("slug", "eventSlug", "ticker"), "")).strip().lower()
    market_count = event_market_count(event)

    title_ok = title == LOCKED_EVENT_TITLE.lower()
    slug_ok = "world-cup-winner" in slug
    # The outright winner event currently contains many team markets.
    # This protects the app from accidentally using a single-match or prop market.
    size_ok = market_count >= 40
    return title_ok and (slug_ok or size_ok)


def fetch_locked_world_cup_event(client: ApiClient) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []

    # 1) Try direct slug routes first. Gamma sometimes changes which slug route works,
    # so this is deliberately defensive.
    for slug in (LOCKED_EVENT_SLUG, "world-cup-winner"):
        event = gamma_event_by_slug(client, slug)
        if event:
            candidates.append(event)

    # 2) Try resolving the full Polymarket URL, but do not accept the result unless
    # it passes the locked World Cup validation below.
    try:
        event, _ = resolve_event(client, DEFAULT_EVENT, limit=20, include_closed=False)
        if event:
            candidates.append(event)
    except Exception:
        pass

    # 3) Search fallback, again validated before use. This avoids silently switching
    # to a different market.
    try:
        candidates.extend(search_events(client, LOCKED_EVENT_TITLE, limit=25, include_closed=False))
    except Exception:
        pass

    valid = [event for event in candidates if event_matches_locked_world_cup(event)]
    if valid:
        # Prefer the candidate with the largest number of markets, which should be
        # the full outright event rather than a related prop.
        valid.sort(key=event_market_count, reverse=True)
        return valid[0]

    seen = []
    for event in candidates[:8]:
        if not isinstance(event, dict):
            continue
        seen.append({
            "title": first_present(event, ("title", "name", "question"), ""),
            "slug": first_present(event, ("slug", "eventSlug", "ticker"), ""),
            "markets": event_market_count(event),
        })
    raise PolymarketLookupError(
        "Could not fetch the locked World Cup Winner event. "
        f"Checked candidates: {seen}"
    )


def fmt_price(value: Any) -> str:
    v = to_float(value)
    return "" if v is None or pd.isna(v) else f"{v:.1f}"


def fmt_decimal(value: Any) -> str:
    v = to_float(value)
    return "" if v is None or pd.isna(v) else f"{v:.3f}"


def fmt_size(value: Any) -> str:
    v = to_float(value)
    return "" if v is None or pd.isna(v) else f"{v:.0f}"


def make_live_view(user_input: str = DEFAULT_EVENT, top_n_book: int = 3, max_teams: int = 48) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    client = ApiClient(timeout=20, pause_seconds=0.0)
    # Locked version: use multiple API lookup routes, but only accept a validated
    # World Cup Winner outright event. This keeps the v3 speed improvements and
    # prevents silent drift to a different market.
    event = fetch_locked_world_cup_event(client)
    event_df, markets_df, prices_df, book_df = build_rows(
        client,
        event,
        top_n_book=top_n_book,
        outcome_filter={"yes"},
        max_markets=60,
        parallel_workers=16,
    )
    yes_ladder_df = build_ladder(markets_df, prices_df, book_df, outcome="Yes", levels=3)
    betfair_view_df = build_betfair_view(yes_ladder_df)
    betfair_view_df = filter_placeholder_teams(betfair_view_df)
    betfair_view_df = filter_qualified_world_cup_teams(betfair_view_df)
    return event, betfair_view_df, markets_df, prices_df, book_df



def is_placeholder_team_name(team: Any) -> bool:
    """Exclude Polymarket placeholder markets such as Team AM / Team AI and Any Other Team."""
    text = str(team or "").strip()
    if not text:
        return True
    if text.lower() == "any other team":
        return True
    if re.fullmatch(r"Team [A-Z]{1,3}", text):
        return True
    return False


def filter_placeholder_teams(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "team" not in df.columns:
        return df
    return df[~df["team"].map(is_placeholder_team_name)].reset_index(drop=True)



QUALIFIED_WORLD_CUP_TEAMS = {
    # Group A
    "mexico", "south africa", "south korea", "czech republic", "czechia",
    # Group B
    "canada", "bosnia and herzegovina", "bosnia-herzegovina", "qatar", "switzerland",
    # Group C
    "brazil", "morocco", "haiti", "scotland",
    # Group D
    "usa", "united states", "united states of america", "paraguay", "australia", "turkiye", "turkey",
    # Group E
    "germany", "curacao", "curaçao", "ecuador", "ivory coast", "cote d'ivoire", "cote d’ivoire", "côte d'ivoire", "côte d’ivoire",
    # Group F
    "netherlands", "japan", "sweden", "tunisia",
    # Group G
    "iran", "new zealand", "belgium", "egypt",
    # Group H
    "spain", "cape verde", "saudi arabia", "uruguay",
    # Group I
    "france", "senegal", "iraq", "norway",
    # Group J
    "argentina", "algeria", "austria", "jordan",
    # Group K
    "portugal", "dr congo", "congo dr", "democratic republic of congo", "uzbekistan", "colombia",
    # Group L
    "england", "croatia", "ghana", "panama",
}


def normalise_team_name(team: Any) -> str:
    text = str(team or "").strip().lower()
    text = text.replace("&", "and")
    text = re.sub(r"\s+", " ", text)
    return text


def is_qualified_world_cup_team(team: Any) -> bool:
    return normalise_team_name(team) in QUALIFIED_WORLD_CUP_TEAMS


def filter_qualified_world_cup_teams(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "team" not in df.columns:
        return df
    return df[df["team"].map(is_qualified_world_cup_team)].reset_index(drop=True)


def sort_view_df(df: pd.DataFrame, sort_mode: str) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if sort_mode == "Best lay (favourite first)":
        out = out.sort_values(["lay_1", "back_1"], ascending=[False, False], na_position="last")
    elif sort_mode == "Team A-Z":
        out = out.sort_values(["team"], ascending=True, na_position="last")
    elif sort_mode == "Team Z-A":
        out = out.sort_values(["team"], ascending=False, na_position="last")
    else:
        out = out.sort_values(["back_1", "lay_1"], ascending=[False, False], na_position="last")
    return out.reset_index(drop=True)


def snapshot_prices(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    cols = ["back_3", "back_2", "back_1", "lay_1", "lay_2", "lay_3"]
    snap: dict[str, dict[str, Any]] = {}
    for _, row in df.iterrows():
        team = str(row.get("team") or "")
        if team:
            snap[team] = {col: row.get(col) for col in cols}
    return snap


def build_change_map(df: pd.DataFrame, previous: dict[str, dict[str, Any]] | None) -> dict[str, dict[str, str]]:
    cols = ["back_3", "back_2", "back_1", "lay_1", "lay_2", "lay_3"]
    changes: dict[str, dict[str, str]] = {}
    if not previous:
        return changes
    for _, row in df.iterrows():
        team = str(row.get("team") or "")
        prev = previous.get(team)
        if not prev:
            continue
        for col in cols:
            cur_v = to_float(row.get(col))
            prev_v = to_float(prev.get(col))
            if cur_v is None or prev_v is None or pd.isna(cur_v) or pd.isna(prev_v):
                continue
            if abs(cur_v - prev_v) < 1e-12:
                continue
            changes.setdefault(team, {})[col] = "up" if cur_v > prev_v else "down"
    return changes


def _excel_number(value: Any) -> float | None:
    v = to_float(value)
    if v is None or pd.isna(v):
        return None
    if v == float("inf") or v == float("-inf"):
        return None
    return v


def _write_num_or_blank(ws: Any, row: int, col: int, value: Any, cell_format: Any) -> None:
    v = _excel_number(value)
    if v is None:
        ws.write_blank(row, col, None, cell_format)
    else:
        ws.write_number(row, col, v, cell_format)


def export_current_view_to_excel_bytes(df: pd.DataFrame) -> bytes:
    """Export the displayed live view in the same 3-row Betfair-style layout."""
    import io

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        workbook = writer.book
        ws = workbook.add_worksheet("CurrentView")
        writer.sheets["CurrentView"] = ws

        fmt_header = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter", "border": 1,
            "bg_color": "#E9EEF5", "font_name": "Calibri", "font_size": 11,
        })
        fmt_team = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter", "border": 1,
            "font_name": "Calibri", "font_size": 11, "indent": 1,
        })
        fmt_back = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter", "border": 1,
            "bg_color": "#CFE2F3", "font_name": "Calibri", "font_size": 11, "num_format": "0.0",
        })
        fmt_lay = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter", "border": 1,
            "bg_color": "#F4CCCC", "font_name": "Calibri", "font_size": 11, "num_format": "0.0",
        })
        fmt_back_dec = workbook.add_format({
            "bold": False, "align": "center", "valign": "vcenter", "border": 1,
            "bg_color": "#CFE2F3", "font_name": "Calibri", "font_size": 11, "num_format": "0.000",
        })
        fmt_lay_dec = workbook.add_format({
            "bold": False, "align": "center", "valign": "vcenter", "border": 1,
            "bg_color": "#F4CCCC", "font_name": "Calibri", "font_size": 11, "num_format": "0.000",
        })
        fmt_size = workbook.add_format({
            "bold": False, "align": "center", "valign": "vcenter", "border": 1,
            "font_name": "Calibri", "font_size": 10, "num_format": "0",
        })
        fmt_blank = workbook.add_format({"border": 1, "font_name": "Calibri", "font_size": 11})

        headers = ["Team", "Back 3", "Back 2", "Back 1", "Lay 1", "Lay 2", "Lay 3"]
        ws.write_row(0, 0, headers, fmt_header)
        ws.freeze_panes(1, 1)
        ws.set_column("A:A", 15)
        ws.set_column("B:G", 13)

        excel_row = 1
        for _, r in df.iterrows():
            ws.write(excel_row, 0, str(r.get("team") or ""), fmt_team)
            _write_num_or_blank(ws, excel_row, 1, r.get("back_3"), fmt_back)
            _write_num_or_blank(ws, excel_row, 2, r.get("back_2"), fmt_back)
            _write_num_or_blank(ws, excel_row, 3, r.get("back_1"), fmt_back)
            _write_num_or_blank(ws, excel_row, 4, r.get("lay_1"), fmt_lay)
            _write_num_or_blank(ws, excel_row, 5, r.get("lay_2"), fmt_lay)
            _write_num_or_blank(ws, excel_row, 6, r.get("lay_3"), fmt_lay)

            ws.write_blank(excel_row + 1, 0, None, fmt_blank)
            ws.write_blank(excel_row + 1, 1, None, fmt_blank)
            ws.write_blank(excel_row + 1, 2, None, fmt_blank)
            _write_num_or_blank(ws, excel_row + 1, 3, r.get("back_1_decimal"), fmt_back_dec)
            _write_num_or_blank(ws, excel_row + 1, 4, r.get("lay_1_decimal"), fmt_lay_dec)
            ws.write_blank(excel_row + 1, 5, None, fmt_blank)
            ws.write_blank(excel_row + 1, 6, None, fmt_blank)

            ws.write_blank(excel_row + 2, 0, None, fmt_blank)
            for c_idx, col_name in enumerate(
                ["back_3_size", "back_2_size", "back_1_size", "lay_1_size", "lay_2_size", "lay_3_size"],
                start=1,
            ):
                _write_num_or_blank(ws, excel_row + 2, c_idx, r.get(col_name), fmt_size)

            excel_row += 3

        # Keep the flat table as a second sheet for checking, but not as the main export view.
        raw_df = df.replace([float("inf"), float("-inf")], pd.NA)
        raw_df.to_excel(writer, sheet_name="RawData", index=False)

    output.seek(0)
    return output.getvalue()


def render_ladder_html(df: pd.DataFrame, max_teams: int = 48, change_map: dict[str, dict[str, str]] | None = None) -> str:
    """Live ladder: 3-row format with all six Back/Lay columns.

    Back 1 and Lay 1 stay larger. Back 3, Back 2, Lay 2 and Lay 3
    use the smaller snapshot-table font.
    """
    css = """
    <style>
      .ladder-wrap { width: 100%; max-width: 100%; overflow-x: auto; }
      table.ladder { border-collapse: collapse; font-family: Calibri, Arial, sans-serif; font-size: 12pt; table-layout: fixed; }
      table.ladder th { border: 1px solid #777; padding: 4px 5px; text-align: center; font-weight: 700; background: #e9eef5; position: sticky; top: 0; z-index: 3; height: 24px; box-sizing: border-box; }
      table.ladder th.small-head { font-size: 10pt; }
      table.ladder th.main-head { font-size: 12pt; }
      table.ladder td { border: 1px solid #c8c8c8; padding: 2px 5px; text-align: center; width: 78px; height: 22px; box-sizing: border-box; }
      table.ladder td.team { text-align: left; font-weight: 700; width: 145px; padding-left: 12px; background: #ffffff; font-size: 12pt; }
      table.ladder td.team-blank { background: #ffffff; width: 145px; }
      table.ladder td.back { background: #CCECFF; font-weight: 700; }
      table.ladder td.lay { background: #F78077; font-weight: 700; }
      table.ladder td.edge-price { font-size: 10pt; }
      table.ladder td.main-price { font-size: 12pt; }
      table.ladder td.back-dec { background: #CCECFF; font-weight: 400; font-size: 12pt; }
      table.ladder td.lay-dec { background: #F78077; font-weight: 400; font-size: 12pt; }
      table.ladder td.size { background: #ffffff; font-size: 9pt; font-weight: 400; }
      table.ladder td.up { box-shadow: inset 0 0 0 9999px rgba(147, 196, 125, 0.65); }
      table.ladder td.down { box-shadow: inset 0 0 0 9999px rgba(255, 229, 153, 0.75); }
      tr.group-start td { border-top: 2px solid #777; }
    </style>
    """
    headers = [
        ("Team", "main-head"),
        ("Back 3", "small-head"), ("Back 2", "small-head"), ("Back 1", "main-head"),
        ("Lay 1", "main-head"), ("Lay 2", "small-head"), ("Lay 3", "small-head"),
    ]
    out = [css, '<div class="ladder-wrap"><table class="ladder"><thead><tr>']
    for h, cls in headers:
        out.append(f'<th class="{cls}">{html.escape(h)}</th>')
    out.append("</tr></thead><tbody>")

    change_map = change_map or {}

    for _, r in df.head(max_teams).iterrows():
        team_raw = str(r.get("team") or "")
        team = html.escape(team_raw)
        team_changes = change_map.get(team_raw, {})
        out.append('<tr class="group-start">')
        out.append(f'<td class="team">{team}</td>')
        for col, cls, size_cls in [
            ("back_3", "back", "edge-price"), ("back_2", "back", "edge-price"), ("back_1", "back", "main-price"),
            ("lay_1", "lay", "main-price"), ("lay_2", "lay", "edge-price"), ("lay_3", "lay", "edge-price"),
        ]:
            extra = f" {team_changes.get(col)}" if team_changes.get(col) else ""
            out.append(f'<td class="{cls} {size_cls}{extra}">{fmt_price(r.get(col))}</td>')
        out.append("</tr>")

        out.append("<tr>")
        out.append('<td class="team-blank"></td>')
        out.append('<td></td><td></td>')
        out.append(f'<td class="back-dec">{fmt_decimal(r.get("back_1_decimal"))}</td>')
        out.append(f'<td class="lay-dec">{fmt_decimal(r.get("lay_1_decimal"))}</td>')
        out.append('<td></td><td></td>')
        out.append("</tr>")

        out.append("<tr>")
        out.append('<td class="team-blank"></td>')
        for col in ["back_3_size", "back_2_size", "back_1_size", "lay_1_size", "lay_2_size", "lay_3_size"]:
            out.append(f'<td class="size">{fmt_size(r.get(col))}</td>')
        out.append("</tr>")

    out.append("</tbody></table></div>")
    return "".join(out)



SNAPSHOT_INTERVAL_SECONDS = 30 * 60
MAX_SNAPSHOT_COLUMNS = 8


def maybe_update_price_history(displayed_df: pd.DataFrame) -> list[dict[str, Any]]:
    """Keep an in-session rolling snapshot history every 30 minutes.

    This does not write to disk. It lasts only while the app session is alive.
    Latest snapshot is kept first so it displays nearest to the live ladder.
    """
    now_ts = time.time()
    history = st.session_state.get("price_history_snapshots", [])
    last_ts = st.session_state.get("last_price_history_ts")

    should_add = last_ts is None or (now_ts - float(last_ts)) >= SNAPSHOT_INTERVAL_SECONDS
    if not should_add:
        return history

    snap_rows: dict[str, dict[str, Any]] = {}
    for _, row in displayed_df.iterrows():
        team = str(row.get("team") or "")
        if not team:
            continue
        snap_rows[team] = {
            "back_1": row.get("back_1"),
            "lay_1": row.get("lay_1"),
            "back_1_decimal": row.get("back_1_decimal"),
            "lay_1_decimal": row.get("lay_1_decimal"),
        }

    snapshot = {
        "timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "label": datetime.now().strftime("%H:%M"),
        "rows": snap_rows,
    }
    history = [snapshot] + list(history)
    history = history[:MAX_SNAPSHOT_COLUMNS]
    st.session_state["price_history_snapshots"] = history
    st.session_state["last_price_history_ts"] = now_ts
    return history


def render_snapshot_history_html(displayed_df: pd.DataFrame, history: list[dict[str, Any]]) -> str:
    """Snapshot history aligned to live ladder: 3 rows per team, Back/Lay columns per snapshot."""
    css = """
    <style>
      .history-wrap { width: 100%; overflow-x: auto; margin-left: 26px; }
      table.history { border-collapse: collapse; font-family: Calibri, Arial, sans-serif; font-size: 10pt; table-layout: fixed; }
      table.history th { border: 1px solid #777; padding: 4px 5px; text-align: center; font-weight: 700; background: #e9eef5; height: 24px; box-sizing: border-box; }
      table.history td { border: 1px solid #c8c8c8; padding: 2px 5px; text-align: center; width: 68px; height: 22px; box-sizing: border-box; }
      table.history td.team { text-align: left; font-weight: 700; width: 145px; padding-left: 12px; background: #ffffff; }
      table.history td.team-blank { background: #ffffff; width: 145px; }
      table.history td.back { font-weight: 700; background: #CCECFF; }
      table.history td.lay { font-weight: 700; background: #F78077; }
      table.history td.back-dec { font-weight: 400; background: #CCECFF; }
      table.history td.lay-dec { font-weight: 400; background: #F78077; }
      table.history td.blank { background: #ffffff; }
      tr.history-spacer td { border-top: none !important; border-bottom: none !important; }
      tr.history-group-start td { border-top: 2px solid #777; }
    </style>
    """
    out = [css, '<div class="history-wrap"><table class="history"><thead><tr>']
    out.append("<th>Team</th>")
    if history:
        for snap in history:
            label = html.escape(str(snap.get("label") or ""))
            out.append(f'<th>{label}<br>Back</th>')
            out.append(f'<th>{label}<br>Lay</th>')
    else:
        out.append('<th>Back</th><th>Lay</th>')
    out.append("</tr></thead><tbody>")

    for _, row in displayed_df.iterrows():
        team_raw = str(row.get("team") or "")
        team = html.escape(team_raw)
        out.append('<tr class="history-group-start">')
        out.append(f'<td class="team">{team}</td>')
        if history:
            for snap in history:
                snap_row = (snap.get("rows") or {}).get(team_raw, {})
                out.append(f'<td class="back">{html.escape(fmt_price(snap_row.get("back_1")))}</td>')
                out.append(f'<td class="lay">{html.escape(fmt_price(snap_row.get("lay_1")))}</td>')
        else:
            out.append('<td class="back"></td><td class="lay"></td>')
        out.append("</tr>")

        out.append("<tr>")
        out.append('<td class="team-blank"></td>')
        if history:
            for snap in history:
                snap_row = (snap.get("rows") or {}).get(team_raw, {})
                out.append(f'<td class="back-dec">{html.escape(fmt_decimal(snap_row.get("back_1_decimal")))}</td>')
                out.append(f'<td class="lay-dec">{html.escape(fmt_decimal(snap_row.get("lay_1_decimal")))}</td>')
        else:
            out.append('<td class="back-dec"></td><td class="lay-dec"></td>')
        out.append("</tr>")

        # Blank third row so each team group lines up with the 3-row live-price layout.
        # The spacer has no top/bottom borders so there is no extra thin line
        # between the decimal row and the next team's thick separator.
        out.append('<tr class="history-spacer">')
        out.append('<td class="team-blank"></td>')
        col_count = len(history) * 2 if history else 2
        for _ in range(col_count):
            out.append('<td class="blank"></td>')
        out.append("</tr>")

    out.append("</tbody></table></div>")
    return "".join(out)

def main_app() -> None:
    st.set_page_config(page_title="Polymarket World Cup Ladder", layout="wide")
    st.markdown("""
    <style>
      [data-testid="stSidebar"] { width: 16rem !important; min-width: 16rem !important; }
      [data-testid="stSidebar"] > div:first-child { width: 16rem !important; min-width: 16rem !important; }
      section.main > div { padding-left: 1rem; padding-right: 1rem; }
    </style>
    """, unsafe_allow_html=True)
    st.title("Polymarket World Cup Winner Ladder — Qualified Teams Live View v16")

    with st.sidebar:
        st.subheader("Settings")
        st.write("Locked event: World Cup Winner")
        st.caption(DEFAULT_EVENT)
        refresh_seconds = st.selectbox("Refresh interval", [15, 30, 60, 120, 300], index=2)
        max_teams = st.slider("Teams shown", min_value=10, max_value=60, value=60, step=1)
        top_n_book = st.slider("Order-book levels fetched", min_value=3, max_value=10, value=3, step=1)
        sort_mode = st.selectbox(
            "Sort order",
            ["Best back (favourite first)", "Best lay (favourite first)", "Team A-Z", "Team Z-A"],
            index=0,
        )
        st.caption("Read-only public Polymarket data. Locked to the World Cup Winner event. Fetches displayed teams only, YES token books only, and fetches books in parallel.")
        st.caption("Snapshot table is in-session only: it records a new column roughly every 30 minutes while the app is awake.")

    st_autorefresh(interval=refresh_seconds * 1000, key="polymarket_refresh")

    status = st.empty()
    try:
        with st.spinner("Fetching latest Polymarket prices..."):
            event, view_df, markets_df, prices_df, book_df = make_live_view(top_n_book=top_n_book, max_teams=max_teams)
    except Exception as exc:
        st.error(f"Could not fetch Polymarket data: {exc}")
        return

    view_df = sort_view_df(view_df, sort_mode)
    displayed_df = view_df.head(max_teams).reset_index(drop=True)
    previous_snapshot = st.session_state.get("previous_price_snapshot", {})
    change_map = build_change_map(displayed_df, previous_snapshot)
    st.session_state["previous_price_snapshot"] = snapshot_prices(displayed_df)
    history = maybe_update_price_history(displayed_df)

    fetched = now_utc_iso()
    title = first_present(event, ("title", "name", "question"), "Matched event")
    slug = first_present(event, ("slug", "ticker"), "")
    status.success(
        f"Updated {fetched} | {title} | markets: {len(markets_df)} | YES-token price rows: {len(prices_df)} | order-book rows: {len(book_df)}"
    )

    st.download_button(
        "Export current screen to Excel",
        data=export_current_view_to_excel_bytes(displayed_df),
        file_name="world_cup_live_view.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    st.caption("Price-change highlighting: green = moved up since previous refresh; yellow = moved down.")

    st.markdown("""
    <style>
      .table-section-title { font-size: 16px; font-weight: 700; margin: 0 0 0.45rem 0; line-height: 1.2; height: 22px; }
      .snapshot-section-title { margin-left: 26px; }
    </style>
    """, unsafe_allow_html=True)

    left_col, right_col = st.columns([3.0, 2.6], gap="large")
    with left_col:
        st.markdown('<div class="table-section-title">Live prices</div>', unsafe_allow_html=True)
        st.markdown(render_ladder_html(displayed_df, max_teams=max_teams, change_map=change_map), unsafe_allow_html=True)
    with right_col:
        st.markdown('<div class="table-section-title snapshot-section-title">30-minute snapshots</div>', unsafe_allow_html=True)
        st.markdown(render_snapshot_history_html(displayed_df, history), unsafe_allow_html=True)

    st.caption("Snapshot table is in-session only. Latest 30-minute snapshot is nearest the live table; older snapshots move right.")

    with st.expander("Raw data checks"):
        st.write("Matched event slug:", slug)
        st.write("Markets:", len(markets_df), "YES-token price rows:", len(prices_df), "Order-book rows:", len(book_df))
        st.dataframe(displayed_df, width="stretch", hide_index=True)


if __name__ == "__main__":
    main_app()

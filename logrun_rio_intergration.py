# logrun_rio_integration.py
#
# Run this file to produce an enriched CSV with raider.io scores.
# The base log-parsing logic is imported from logrun.py — any changes
# there are automatically reflected here.

import os
import re
import csv
import time
import json
import traceback
import requests
from datetime import datetime, timezone

# ── import everything we need from the base script ──────────────────────────
from logrun import (
    LOGS_FOLDER,
    process_file,
    parse_log,
    parse_damage_healing_flexible,
    parse_damage_healing_tables,
    compute_log_fingerprint,
    get_sort_dt,
    compute_score_upgrades,
    get_csv_headers,
    print_milestone_summary,
)

# ---------------------------------------------------------------------------
# Output file (different from logrun.py's logsummary.csv)
# ---------------------------------------------------------------------------

OUTPUT_FILE = "logsummary_rio.csv"

# ---------------------------------------------------------------------------
# Raider.io configuration
# ---------------------------------------------------------------------------

CURRENT_SEASON    = "season-mn-1"
REALM_CACHE_FILE  = "log_player_realm_cache.json"
RIO_PLAYER_URL    = "https://raider.io/api/v1/characters/profile"
RIO_FIELDS        = f"mythic_plus_scores_by_season:{CURRENT_SEASON}"

# ---------------------------------------------------------------------------
# Score cache configuration
# ---------------------------------------------------------------------------

# How old (in hours) a cached score can be before it is re-fetched.
# Set to 24 so that multiple runs on the same day reuse cached scores.
SCORE_CACHE_MAX_AGE_HOURS = 24

# ---------------------------------------------------------------------------
# EU realms ordered roughly by population (most populated first).
# Names must match exactly what raider.io uses as realm slugs resolve
# to these display names internally.
EU_REALMS = [
    # Top population realms
    "Tarren Mill", "Dentarg",                           # Connected Tarren Mill
    "Kazzak",
    "Draenor",
    "Twisting Nether",
    "Silvermoon",
    "Hyjal",
    "Ragnaros",
    "Ravencrest",
    "Blackrock",
    "Mal'Ganis", "Blackhand", "Echsenkessel", "Taerar", # Connected Mal'Ganis
    "Al'Akir", "Burning Legion", "Skullcrusher", "Xavius",  # Connected Al'Akir
    "Blackmoore", "Lordaeron", "Tichondrius",           # Connected Blackmoore
    "Sanguino", "Shen'dralar", "Uldum", "Zul'jin",      # Connected Sanguino
    "Ysondre",
    "Stormscale",
    "Eredar",
    "Antonidas",
    "Kargath", "Ambossar", "Thrall",                    # Connected Kargath
    "Drak'thul", "Burning Blade",                       # Connected Drak'thul
    "Argent Dawn",
    "Nemesis",
    "Dalaran", "Cho'gall", "Eldre'Thalas",              # Connected Dalaran
    "Marécage de Zangar", "Sinstralis",                 # Connected Dalaran continued
    "Sylvanas", "Auchindoun", "Dunemaul", "Jaedenar",   # Connected Sylvanas
    "Confrérie du Thorium", "Conseil des Ombres",       # Connected Confrérie du Thorium
    "Culte de la Rive noire", "Kirin Tor",              # Connected Confrérie du Thorium continued
    "La Croisade écarlate", "Les Clairvoyants",         # Connected Confrérie du Thorium continued
    "Les Sentinelles",                                  # Connected Confrérie du Thorium continued
    "Doomhammer", "Turalyon",                           # Connected Doomhammer
    "Elune", "Varimathras",                             # Connected Elune
    "Zirkel des Cenarius", "Der Mithrilorden",          # Connected Zirkel des Cenarius
    "Der Rat von Dalaran", "Die Nachtwache",            # Connected Zirkel des Cenarius continued
    "Forscherliga", "Todeswache",                       # Connected Zirkel des Cenarius continued
    "Kilrogg", "Arathor", "Hellfire", "Nagrand",        # Connected Kilrogg
    "Runetotem",                                        # Connected Kilrogg continued
    "Kael'thas", "Arak-arahm", "Rashgarroth",           # Connected Kael'Thas
    "Throk'Feroth",                                     # Connected Kael'Thas continued
    "Anub'arak", "Aman'thul", "Dalvengyr",              # Connected Anub'arak
    "Frostmourne", "Nazjatar", "Zuluhed",               # Connected Anub'arak continued
    "Emerald Dream", "Terenas",                         # Connected Emerald Dream
    "Shattered Hand", "Bloodfeather", "Burning Steppes",# Connected Shattered Hand
    "Darkspear", "Executus", "Kor'gall", "Saurfang",    # Connected Shattered Hand continued
    "Terokkar",                                         # Connected Shattered Hand continued
    "Dun Modr", "C'Thun",                               # Connected Dun Modr
    "Defias Brotherhood", "Darkmoon Faire",             # Connected Defias Brotherhood
    "Earthen Ring", "Ravenholdt", "Scarshield Legion",  # Connected Defias Brotherhood continued
    "Sporeggar", "The Venture Co",                      # Connected Defias Brotherhood continued
    "Uldaman", "Drek'Thar", "Eitrigg", "Krasus",        # Connected Uldaman
    "Ahn'Qiraj", "Balnazzar", "Boulderfist",            # Connected Sunstrider
    "Chromaggus", "Daggerspine", "Laughing Skull",      # Connected Sunstrider continued
    "Shattered Halls", "Sunstrider", "Talnivarr",       # Connected Sunstrider continued
    "Trollbane",                                        # Connected Sunstrider continued
    "Outland",
    "Well of Eternity", "Pozzo dell'Eternità",          # Well of Eternity
    "Azjol-Nerub", "Quel'Thalas",                       # Connected Azjol-Nerub
    "Magtheridon",
    "Sargeras", "Garona", "Ner'zhul",                   # Connected Sargeras
    "Aggramar", "Hellscream",                           # Connected Aggramar
    "Grim Batol", "Aggra (Português)", "Frostmane",     # Connected Grim Batol
    "Frostwolf",
    "Stormreaver", "Dragonmaw", "Haomarush",            # Connected Stormreaver
    "Spinebreaker", "Vashj",                            # Connected Stormreaver continued
    "Azshara", "Baelgun", "Krag'jin", "Lothar",         # Connected Lothar (Azshara group)
    "Malfurion", "Malygos",                             # Connected Malfurion
    "Dun Morogh", "Norgannon",                          # Connected Dun Morogh
    "Stormrage", "Azuremyst",                           # Connected Stormrage
    "Nordrassil", "Bronze Dragonflight",                # Connected Nordrassil
    "Shadowsong", "Aszune",                             # Connected Shadowsong
    "Alleria", "Rexxar",                                # Connected Alleria
    "Arthas", "Blutkessel", "Durotan",                  # Connected Arthas
    "Kel'Thuzad", "Tirion", "Vek'lor", "Wrathbringer",  # Connected Arthas continued
    "Alexstrasza", "Madmortem", "Nethersturm",          # Connected Alexstrasza
    "Proudmoore",                                       # Connected Alexstrasza continued
    "Garrosh", "Nozdormu", "Perenolde",                 # Connected Garrosh
    "Shattrath", "Teldrassil",                          # Connected Garrosh continued
    "Destromath", "Gilneas", "Gorgonnash",              # Connected Destromath
    "Mannoroth", "Nefarian", "Nera'thor", "Ulduar",     # Connected Destromath continued
    "Aerie Peak", "Blade's Edge", "Bronzebeard",        # Connected Eonar
    "Eonar", "Vek'nilash",                              # Connected Eonar continued
    "Anetheron", "Festung der Stürme", "Gul'dan",       # Connected Anetheron
    "Kil'jaeden", "Nathrezim", "Rajaxx",                # Connected Anetheron continued
    "The Maelstrom", "Deathwing", "Dragonblight",       # Connected The Maelstrom
    "Ghostlands", "Karazhan", "Lightning's Blade",      # Connected The Maelstrom continued
    "Alonsus", "Anachronos", "Kul Tiras",               # Connected Alonsus
    "Thunderhorn", "Wildhammer",                        # Connected Thunderhorn
    "Bloodhoof", "Khadgar",                             # Connected Bloodhoof
    "Lightbringer", "Mazrigos",                         # Connected Lightbringer
    "Twilight's Hammer", "Agamaggan", "Bloodscalp",     # Connected Twilight's Hammer
    "Crushridge", "Emeriss", "Hakkar",                  # Connected Twilight's Hammer continued
    "Frostwhisper", "Bladefist", "Darksorrow",          # Connected Frostwhisper
    "Genjuros", "Neptulon", "Zenedar",                  # Connected Frostwhisper continued
    "Khaz'goroth", "Arygos",                            # Connected Khaz'goroth
    "Ysera", "Malorne",                                 # Connected Ysera
    "Chants éternels", "Vol'jin",                       # Connected Chants éternels
    "Medivh", "Suramar",                                # Connected Medivh
    "Illidan", "Arathi", "Naxxramas", "Temple noir",    # Connected Illidan
    "Tyrande", "Colinas Pardas", "Los Errantes",        # Connected Tyrande
    "Exodar", "Minahonda",                              # Connected Exodar
    "Moonglade", "Steamwheedle Cartel", "The Sha'tar",  # Connected Moonglade
    "Area 52", "Sen'jin", "Un'Goro",                    # Connected Area 52
    "Kult der Verdammten", "Das Konsortium",            # Connected Kult der Verdammten
    "Das Syndikat", "Der Abyssische Rat",               # Connected Kult der Verdammten continued
    "Die Arguswacht", "Die Silberne Hand",              # Connected Kult der Verdammten continued
    "Die Todeskrallen", "Die ewige Wacht",              # Connected Kult der Verdammten continued
    "Die Aldor",
    "Onyxia", "Dethecus", "Mug'thol", "Terrordar",     # Connected Onyxia
    "Theradras",                                        # Connected Onyxia continued
    "Khaz Modan",
    "Chamber of Aspects",
    "Aegwynn",
    # Russian realms
    "Ревущий фьорд",       # Howling Fjord
    "Гордунни",            # Gordunni
    "Свежеватель Душ",     # Soulflayer
    "Вечная Песня",        # Eversong
    "Пиратская Бухта",     # Booty Bay
    "Черный Шрам",         # Blackscar
    "Борейская тундра",    # Borean Tundra
    "Ткач Смерти",         # Deathweaver
    "Гром",                # Grom
    "Термоштепсель",       # Thermaplugg
    "Подземье",            # Deepholm
    "Галакронд",           # Galakrond
    "Разувий",             # Razuvious
    "Голдринн",            # Goldrinn
    "Седогрив",            # Greymane
    "Король-лич",          # Lich King
    "Азурегос",            # Azuregos
    "Дракономор",          # Fordragon
    "Ясеневый лес",        # Ashenvale
    "Страж Смерти",        # Deathguard
]

# ---------------------------------------------------------------------------
# Realm cache helpers
# ---------------------------------------------------------------------------

def load_realm_cache():
    if os.path.isfile(REALM_CACHE_FILE):
        try:
            with open(REALM_CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_realm_cache(cache):
    with open(REALM_CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Score cache helpers
#
# The score cache is a dict stored as JSON with the shape:
#   {
#     "PlayerName": {
#       "score":      1234.5,
#       "fetched_at": "2024-01-15T14:30:00+00:00"   ← UTC ISO-8601
#     },
#     ...
#   }
#
# `fetched_at` is always stored in UTC so the age calculation is
# unambiguous regardless of the machine's local timezone.
# ---------------------------------------------------------------------------

SCORE_CACHE_FILE = "log_player_score_cache.json"


def load_score_cache():
    """Load the score cache from disk.  Returns an empty dict on any error."""
    if os.path.isfile(SCORE_CACHE_FILE):
        try:
            with open(SCORE_CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_score_cache(cache):
    """Persist the score cache to disk."""
    with open(SCORE_CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def _score_cache_is_fresh(entry):
    """
    Return True if the cache entry was fetched within SCORE_CACHE_MAX_AGE_HOURS.

    An entry is considered stale (and therefore should be re-fetched) if:
      • it has no 'fetched_at' key, or
      • the timestamp cannot be parsed, or
      • now() − fetched_at  >  SCORE_CACHE_MAX_AGE_HOURS
    """
    fetched_at_str = entry.get("fetched_at")
    if not fetched_at_str:
        return False
    try:
        fetched_at = datetime.fromisoformat(fetched_at_str)
        # Ensure it is timezone-aware so we can compare with utcnow.
        if fetched_at.tzinfo is None:
            fetched_at = fetched_at.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - fetched_at).total_seconds() / 3600
        return age_hours < SCORE_CACHE_MAX_AGE_HOURS
    except Exception:
        return False


def _make_score_entry(score):
    """Build a score-cache entry stamped with the current UTC time."""
    return {
        "score":      score,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Class name normalisation
# ---------------------------------------------------------------------------

def _normalize_class(cls_str):
    """
    Remove all spaces, apostrophes, hyphens and underscores, then lowercase.

    Examples that all become identical after normalisation:
        "Demon Hunter"  →  "demonhunter"
        "DemonHunter"   →  "demonhunter"
        "DEMONHUNTER"   →  "demonhunter"
        "Monk"          →  "monk"
        "Evoker"        →  "evoker"
    """
    return re.sub(r"[\s'\-_]", "", cls_str).lower()


# ---------------------------------------------------------------------------
# Raider.io API helpers
# ---------------------------------------------------------------------------

def _rio_lookup(name, realm, region="eu"):
    """
    Fetch a raider.io character profile.
    Returns (http_status_code, parsed_json_or_None).
    """
    params = {
        "region": region,
        "realm":  realm,
        "name":   name,
        "fields": RIO_FIELDS,
    }
    try:
        resp = requests.get(RIO_PLAYER_URL, params=params, timeout=8)
        try:
            body = resp.json()
        except Exception:
            body = None
        return resp.status_code, (body if resp.status_code == 200 else None)
    except Exception as exc:
        print(f"\n  [request error] {exc}")
        return None, None


def _get_current_score(rio_data):
    """Extract the current-season 'all' score from a raider.io profile dict."""
    if not rio_data:
        return 0
    for entry in rio_data.get("mythic_plus_scores_by_season", []):
        if entry.get("season") == CURRENT_SEASON:
            return entry.get("scores", {}).get("all", 0)
    return 0


def _class_matches(rio_data, expected_class):
    """
    Compare the raider.io class against the class recorded in the log.

    Both sides are normalised (spaces / punctuation removed, lowercased)
    so "Demon Hunter" == "DemonHunter" == "DEMONHUNTER", etc.

    If either value is missing the check is skipped (returns True).
    """
    if not rio_data or not expected_class:
        return True
    rio_class = rio_data.get("class", "")
    if not rio_class:
        return True
    return _normalize_class(rio_class) == _normalize_class(expected_class)


# ---------------------------------------------------------------------------
# Realm discovery
# ---------------------------------------------------------------------------

def find_realm_for_player(name, expected_class, rate_limit_delay=0.33):
    """
    Iterate over EU_REALMS until we find a realm where `name` exists
    AND the raider.io class matches `expected_class`.

    Score is NOT used as a filter — a player may simply not have played
    this season yet.

    Returns (realm_name, score) or (None, 0) if not found.
    """
    for realm in EU_REALMS:
        time.sleep(rate_limit_delay)
        status, data = _rio_lookup(name, realm)

        if status == 429:
            print(f"\n  [rate-limit] 429 on {name}@{realm}; sleeping 30 s …")
            time.sleep(30)
            status, data = _rio_lookup(name, realm)

        if status != 200 or data is None:
            # 404 = character not on this realm, keep searching
            continue

        rio_class = data.get("class", "")
        if not _class_matches(data, expected_class):
            # Found a character with this name but wrong class —
            # could be a name collision on a different realm, keep searching.
            print(
                f"\n  [class mismatch] {name}@{realm}: "
                f"rio='{rio_class}' log='{expected_class}' — skipping"
            )
            continue

        # Character found with the correct class
        score = _get_current_score(data)
        return realm, score

    return None, 0


# ---------------------------------------------------------------------------
# Player helpers
# ---------------------------------------------------------------------------

def collect_unique_players(rows):
    """
    Return { player_name: class_str } for every player seen across all rows.
    The class value comes from player{N}_class in the CSV, e.g. "Monk",
    "Demon Hunter", "Evoker" — exactly as the log recorded it.
    """
    players = {}
    for row in rows:
        for idx in range(1, 6):
            name = row.get(f"player{idx}_name",  "").strip()
            cls  = row.get(f"player{idx}_class", "").strip()
            if name:
                players[name] = cls   # last-seen class wins (stable)
    return players


def resolve_player_realms(unique_players, realm_cache):
    """
    For every player not already in realm_cache, attempt realm discovery.
    Players with a blank realm are treated as manually marked unfindable
    and are silently skipped.
    Updates realm_cache in-place and persists after each successful find.
    """
    # A player is considered handled if they appear in the cache at all,
    # even with an empty realm (manually marked as unfindable).
    unknown = {n: c for n, c in unique_players.items() if n not in realm_cache}
    if not unknown:
        print("All players already resolved in realm cache.")
        return

    print(f"\nResolving realms for {len(unknown)} new player(s) …")
    for name, cls in unknown.items():
        print(f"  Searching: {name} ({cls}) …", end=" ", flush=True)
        realm, score = find_realm_for_player(name, cls)
        if realm:
            realm_cache[name] = {"realm": realm, "class": cls}
            save_realm_cache(realm_cache)
            print(f"found on {realm}  (score {score:.0f})")
        else:
            print("NOT FOUND (will retry next run)")


def fetch_current_scores(unique_players, realm_cache, score_cache,
                         rate_limit_delay=1.2):
    """
    Return { player_name: score_float } for every player whose realm is known.

    Players present in the cache with a blank realm are treated as manually
    marked unfindable — they are counted and reported but no API call is
    attempted and they receive no score entry.

    For each player with a known realm the function first checks the score
    cache:
      • If a fresh entry exists (age < SCORE_CACHE_MAX_AGE_HOURS) it is
        used as-is and no API call is made.
      • Otherwise the score is re-fetched from raider.io, the cache entry
        is updated, and the cache is saved to disk.

    `score_cache` is mutated in-place so the caller can persist it after
    this function returns (the function also saves it internally after every
    successful fetch, mirroring the realm-cache pattern).
    """
    # Split cache entries for players we've seen into two buckets.
    known   = {}   # realm is a non-empty string → fetchable
    skipped = {}   # realm is blank → manually marked unfindable

    for name in unique_players:
        if name not in realm_cache:
            continue
        info = realm_cache[name]
        if info.get("realm", "").strip():
            known[name]   = info
        else:
            skipped[name] = info

    if skipped:
        print(
            f"\nSkipping {len(skipped)} manually marked unfindable "
            f"player(s): {', '.join(sorted(skipped))}"
        )

    fresh_count = sum(
        1 for n in known
        if _score_cache_is_fresh(score_cache.get(n, {}))
    )
    stale_count = len(known) - fresh_count

    print(
        f"\nFetching scores for {len(known)} player(s) "
        f"({fresh_count} cached, {stale_count} to fetch) …"
    )

    scores = {}
    for name, info in known.items():
        cached_entry = score_cache.get(name, {})

        if _score_cache_is_fresh(cached_entry):
            # ── cache hit ────────────────────────────────────────────────
            scores[name] = cached_entry["score"]
            print(
                f"  {name} ({info['realm']}): "
                f"{scores[name]:.0f}  [cached]"
            )
            continue

        # ── cache miss / stale — fetch from raider.io ────────────────────
        realm = info["realm"]
        time.sleep(rate_limit_delay)
        status, data = _rio_lookup(name, realm)

        if status == 429:
            print(
                f"\n  [rate-limit] 429 on score fetch for {name}; "
                f"sleeping 30 s …"
            )
            time.sleep(30)
            status, data = _rio_lookup(name, realm)

        score = _get_current_score(data) if (status == 200 and data) else 0
        scores[name] = score

        # Update the cache entry and persist immediately so a crash or
        # keyboard-interrupt mid-run doesn't discard work already done.
        score_cache[name] = _make_score_entry(score)
        save_score_cache(score_cache)

        print(f"  {name} ({info['realm']}): {score:.0f}  [fetched]")

    return scores


# ---------------------------------------------------------------------------
# CSV enrichment helpers
# ---------------------------------------------------------------------------

def inject_scores_into_rows(rows, player_scores):
    """Add a player{N}_rio_score column to every row."""
    for row in rows:
        for idx in range(1, 6):
            name = row.get(f"player{idx}_name", "").strip()
            row[f"player{idx}_rio_score"] = (
                player_scores.get(name, "") if name else ""
            )


def get_csv_headers_with_scores():
    """
    Build the full header list by inserting a rio_score column immediately
    after each player's healthstones column.
    """
    extended = []
    for col in get_csv_headers():
        extended.append(col)
        if col.endswith("_healthstones"):
            prefix = col[: col.index("_healthstones")]   # e.g. "player3"
            extended.append(f"{prefix}_rio_score")
    return extended


# ---------------------------------------------------------------------------
# Quick smoke-test — run this block in isolation to verify API calls work
# before processing the full log set.
# Usage:  python logrun_rio_integration.py --test
# ---------------------------------------------------------------------------

def _smoke_test():
    """
    Directly look up the two known-problem players to confirm
    the API calls and class matching work correctly.
    """
    test_cases = [
        ("Fatmonka",     "Monk",          "blackhand"),
        ("Powerpegging", "Evoker",        "ravencrest"),
        ("Psychobunny",  "Demon Hunter",  "ravencrest"),
    ]
    print("\n=== Smoke test ===")
    for name, log_class, realm in test_cases:
        status, data = _rio_lookup(name, realm)
        print(f"\n{name} @ {realm}")
        print(f"  HTTP status      : {status}")
        if data:
            rio_class = data.get("class", "N/A")
            score     = _get_current_score(data)
            match     = _class_matches(data, log_class)
            print(f"  raider.io class  : {rio_class!r}")
            print(f"  log class        : {log_class!r}")
            print(f"  normalised rio   : {_normalize_class(rio_class)!r}")
            print(f"  normalised log   : {_normalize_class(log_class)!r}")
            print(f"  class match      : {match}")
            print(f"  season score     : {score}")
        else:
            print("  No data returned (character not found or API error)")
    print("\n=== End smoke test ===\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not os.path.isdir(LOGS_FOLDER):
        print(f"Error: folder '{LOGS_FOLDER}' not found.")
        return

    log_files = [
        os.path.join(LOGS_FOLDER, f)
        for f in sorted(os.listdir(LOGS_FOLDER))
        if os.path.isfile(os.path.join(LOGS_FOLDER, f))
    ]

    if not log_files:
        print(f"No files found in '{LOGS_FOLDER}'.")
        return

    # ── parse logs ───────────────────────────────────────────────────────────
    rows = []
    seen_fingerprints = {}   # fingerprint -> filepath

    for fp in log_files:
        try:
            row = process_file(fp)
            if not row:
                continue

            # Re-parse just enough to compute fingerprint
            with open(fp, 'r', encoding='utf-8') as f:
                text = f.read()
            data = parse_log(text)
            dmg, heal = parse_damage_healing_flexible(text)
            if not dmg or not heal:
                dmg2, heal2 = parse_damage_healing_tables(text)
                if not dmg:
                    dmg  = dmg2
                if not heal:
                    heal = heal2
            data['damage_done']  = dmg
            data['healing_done'] = heal
            data['deaths']       = data.get('deaths', [])

            fp_hash = compute_log_fingerprint(data)

            if fp_hash in seen_fingerprints:
                print(
                    f"\n  WARNING: Duplicate stats detected!\n"
                    f"    File      : {fp}\n"
                    f"    Matches   : {seen_fingerprints[fp_hash]}\n"
                    f"    Dungeon   : {row.get('dungeon')} +{row.get('key_level')}\n"
                    f"    The file above likely has stale data from the earlier run.\n"
                    f"    Skipping  : {fp}"
                )
                continue

            seen_fingerprints[fp_hash] = fp
            rows.append(row)

        except Exception as e:
            print(f"  ERROR in {fp}: {e}")
            traceback.print_exc()

    # Sort newest → oldest
    rows.sort(key=lambda r: get_sort_dt(r.get('timestamp', '')), reverse=True)

    upgrade_values = compute_score_upgrades(rows)
    for row, val in zip(rows, upgrade_values):
        row['IsScoreUpgrade'] = val

    # ── raider.io enrichment ─────────────────────────────────────────────────
    realm_cache    = load_realm_cache()
    score_cache    = load_score_cache()
    unique_players = collect_unique_players(rows)

    resolve_player_realms(unique_players, realm_cache)
    player_scores = fetch_current_scores(unique_players, realm_cache, score_cache)
    inject_scores_into_rows(rows, player_scores)

    # ── write enriched CSV ───────────────────────────────────────────────────
    headers = get_csv_headers_with_scores()
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n{'='*60}")
    print(f"Written {len(rows)} rows to {OUTPUT_FILE}")
    print_milestone_summary(OUTPUT_FILE, player_scores=player_scores)


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        _smoke_test()
    else:
        main()
import re
import csv
import os
import traceback
import hashlib
from datetime import datetime

LOGS_FOLDER = "logged_runs"
OUTPUT_FILE = "logsummary.csv"

NAME_CHARS = r"[\w\u0400-\u04FF\u00D8\u00F8\u00C6\u00E6\u00C5\u00E5'\-\.]"

# ---------------------------------------------------------------------------
# Dungeon timers (seconds)
# ---------------------------------------------------------------------------

DUNGEON_TIMERS = {
    "Magisters' Terrace":      34 * 60,
    "Maisara Caverns":         33 * 60,
    "Nexus-Point Xenas":       30 * 60,
    "Windrunner Spire":        33 * 60 + 30,
    "Algeth'ar Academy":       31 * 60,
    "Pit of Saron":            30 * 60,
    "Seat of the Triumvirate": 34 * 60,
    "Skyreach":                28 * 60,
}


def duration_to_seconds(duration_str):
    if not duration_str:
        return 0
    parts = duration_str.split(':')
    try:
        return int(parts[0]) * 60 + int(parts[1])
    except (ValueError, IndexError):
        return 0


def get_dungeon_timer(dungeon_name):
    if not dungeon_name:
        return None
    if dungeon_name in DUNGEON_TIMERS:
        return DUNGEON_TIMERS[dungeon_name]
    lower = dungeon_name.lower()
    for key, val in DUNGEON_TIMERS.items():
        if key.lower() == lower:
            return val
    for key, val in DUNGEON_TIMERS.items():
        if key.lower() in lower or lower in key.lower():
            return val
    return None


# ---------------------------------------------------------------------------
# Completion logic
# ---------------------------------------------------------------------------

def estimate_completion(dungeon_name, duration_str, deaths):
    timer_secs    = get_dungeon_timer(dungeon_name)
    duration_secs = duration_to_seconds(duration_str)

    if timer_secs is None or duration_secs == 0:
        return ''

    # Timed out
    if duration_secs > timer_secs:
        return 0

    ratio = duration_secs / timer_secs

    # Too short to be a real completion
    if ratio < 0.60:
        return 0

    # 60–100% of timer: inspect deaths in final 5 minutes
    last_5_threshold = duration_secs - 300

    def ts_to_secs(ts):
        parts = ts.split(':')
        try:
            return int(parts[0]) * 60 + int(parts[1])
        except (ValueError, IndexError):
            return 0

    late_dead = set()
    for d in deaths:
        if ts_to_secs(d.get('timestamp', '')) >= last_5_threshold:
            late_dead.add(d['player'])

    if len(late_dead) >= 4:
        return 0

    return 1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_datetime_combined(date_str, time_str):
    date_str = date_str.strip()
    time_str = time_str.strip()
    combined = f"{date_str} {time_str}"
    for fmt in ('%a %b %d %Y %I:%M %p', '%a %b %d %Y %I:%M%p'):
        try:
            dt = datetime.strptime(combined, fmt)
            return dt.strftime('%Y-%m-%d %H:%M'), dt
        except ValueError:
            continue
    return f"{date_str} {time_str}", datetime.min


def get_sort_dt(timestamp_str):
    try:
        return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M')
    except ValueError:
        return datetime.min


def amount_to_float(amount_str):
    """
    Convert '25.75m', '619.3k', '1,234,567', '176,653.6' etc. to float.
    Note: values like '176,653.6' are raw numbers with comma thousands separators,
    NOT amounts with a suffix — handle carefully.
    """
    if not amount_str:
        return 0.0
    s = amount_str.strip().replace(',', '')
    multiplier = 1.0
    if s.lower().endswith('b'):
        multiplier = 1_000_000_000.0
        s = s[:-1]
    elif s.lower().endswith('m'):
        multiplier = 1_000_000.0
        s = s[:-1]
    elif s.lower().endswith('k'):
        multiplier = 1_000.0
        s = s[:-1]
    try:
        return float(s) * multiplier
    except ValueError:
        return 0.0


# ---------------------------------------------------------------------------
# Top-level log parser
# ---------------------------------------------------------------------------

def parse_log(text):
    data = {}

    # --- Uploader ---
    created_match = re.search(r'Created by (\S+)', text)
    if created_match:
        data['uploader'] = created_match.group(1)
    else:
        personal_match = re.search(r'(\S+)\s*\nPersonal Logs', text)
        data['uploader'] = personal_match.group(1) if personal_match else ''

    # --- Date ---
    date_match = re.search(
        r'Created by \S+ on ((?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) \w+ \d{1,2} \d{4})',
        text
    )
    data['log_date'] = date_match.group(1).strip() if date_match else ''

    # --- Dungeon, key level, duration, time-of-day ---
    dungeon_found = False

    m = re.search(
        r'([\w\u0400-\u04FF\u00D8\u00F8\u00C6\u00E6\u00C5\u00E5 \'\-]+?)\+'
        r'\s*\n'
        r'(?:Last Run\s*[-–]\s*)?'
        r'Level\s+(\d+)\s+'
        r'((?:\[[^\]]+\]\s*)*)'
        r'\d*\s*'
        r'\((\d+:\d+)\)'
        r'\s*([\d:]+\s*[AP]M)',
        text
    )
    if m:
        data['dungeon']     = m.group(1).strip()
        data['key_level']   = m.group(2).strip()
        data['duration']    = m.group(4).strip()
        data['time_of_day'] = m.group(5).strip()
        dungeon_found = True

    if not dungeon_found:
        m2 = re.search(
            r'Last Run\s*[-–]\s*Level\s+(\d+)\s+'
            r'((?:\[[^\]]+\]\s*)*)'
            r'\d*\s*'
            r'\((\d+:\d+)\)'
            r'\s*([\d:]+\s*[AP]M)',
            text
        )
        if m2:
            before = text[:m2.start()]
            dn = re.search(
                r'([\w\u0400-\u04FF\u00D8\u00F8\u00C6\u00E6\u00C5\u00E5 \'\-]+?)\+\s*\n?\s*$',
                before
            )
            data['dungeon']     = dn.group(1).strip() if dn else ''
            data['key_level']   = m2.group(1).strip()
            data['duration']    = m2.group(3).strip()
            data['time_of_day'] = m2.group(4).strip()
            dungeon_found = True

    if not dungeon_found:
        data['dungeon']     = ''
        data['key_level']   = ''
        data['duration']    = ''
        tod = re.search(r'(\d{1,2}:\d{2}\s*[AP]M)', text)
        data['time_of_day'] = tod.group(1).strip() if tod else ''

    if data.get('log_date') and data.get('time_of_day'):
        data['timestamp'], data['_sort_dt'] = parse_datetime_combined(
            data['log_date'], data['time_of_day']
        )
    else:
        data['timestamp'] = data.get('time_of_day', '')
        data['_sort_dt']  = datetime.min

    data['deaths']  = parse_deaths(text)
    data['players'] = parse_characters_direct(text)

    return data


# ---------------------------------------------------------------------------
# Deaths
# ---------------------------------------------------------------------------

def parse_deaths(text):
    deaths = []
    deaths_match = re.search(
        r'Deaths\s*\nName\s*\n\s*Killing Blow\s*\n\s*Time\s*\n(.*?)(?=Characters)',
        text, re.DOTALL
    )
    if not deaths_match:
        return deaths

    block = deaths_match.group(1)
    name_pat = NAME_CHARS + r'+'
    for entry in re.findall(
        r'^(' + name_pat + r')\s+(.+?)\s+(\d+:\d+)\s*$',
        block, re.MULTILINE
    ):
        deaths.append({
            'player':       entry[0].strip(),
            'killing_blow': entry[1].strip(),
            'timestamp':    entry[2].strip(),
        })
    return deaths


# ---------------------------------------------------------------------------
# Characters / Players
# ---------------------------------------------------------------------------

CLASS_PATTERN = (
    r'(Monk|Death Knight|Demon Hunter|Evoker|Druid|Warrior|Paladin|'
    r'Hunter|Rogue|Priest|Shaman|Mage|Warlock)'
)


def parse_characters_direct(text):
    players = []
    player_re = re.compile(
        r'^(' + NAME_CHARS + r'+)\s*\n'
        + CLASS_PATTERN +
        r'\s+\(([^)]+)\)\s*\n'
        r'(\d+)\s+Item Level',
        re.MULTILINE
    )
    matches = list(player_re.finditer(text))
    for i, m in enumerate(matches):
        name       = m.group(1).strip()
        char_class = m.group(2).strip()
        spec       = m.group(3).strip()
        ilvl       = m.group(4).strip()

        search_start = m.end()
        search_end   = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        player_block = text[search_start:search_end]

        potions, healthstones = parse_consumables(player_block)
        players.append({
            'name':         name,
            'class':        char_class,
            'spec':         spec,
            'ilvl':         ilvl,
            'potions':      potions,
            'healthstones': healthstones,
            'role':         get_role(text, name),
        })

    # Reorder so Powerpegging is always at index 1 (player2) if present
    MY_NAME = "Powerpegging"
    names = [p['name'] for p in players]
    if MY_NAME in names:
        idx = names.index(MY_NAME)
        me = players.pop(idx)
        players.insert(1, me)

    return players



def get_role(text, name):
    tanks_m   = re.search(r'Tanks:\s*(.+)',   text)
    healers_m = re.search(r'Healers:\s*(.+)', text)
    dps_m     = re.search(r'DPS:\s*(.+)',     text)
    if tanks_m and name in tanks_m.group(1):
        return 'Tank'
    if healers_m and name in healers_m.group(1):
        return 'Healer'
    if dps_m     and name in dps_m.group(1):
        return 'DPS'
    return ''


def parse_consumables(block):
    m = re.search(r'\t\s*(\d+)\s*\t\s*(\d+)\s*$', block, re.MULTILINE)
    if m:
        return m.group(1), m.group(2)
    m = re.search(r'^\s*(\d+)\s+\t\s*(\d+)\s*$', block, re.MULTILINE)
    if m:
        return m.group(1), m.group(2)
    m = re.search(r'(\d+)\s{1,6}(\d+)\s*$', block.strip())
    if m:
        return m.group(1), m.group(2)
    return '0', '0'


# ---------------------------------------------------------------------------
# Damage / Healing table parsing — handles all three observed formats
#
# Format A (percentage on its own line):
#   PlayerName
#   34.08%
#   25.75m
#   307,767.0
#
# Format B (no percentage, amount and rate on separate lines):
#   PlayerName
#   34.98m
#   169,738.8
#
# Format C (no percentage, amount and rate on SAME line, tab-separated):
#   PlayerName
#   280.76m \t 176,653.6
# ---------------------------------------------------------------------------

# Matches an "amount" token: digits/commas, optional decimal, optional k/m/b suffix
# e.g.  25.75m  |  619.3k  |  280.76m  |  1,234,567
_AMOUNT_RE = re.compile(r'^[\d,]+(?:\.\d+)?[kmb]?$', re.IGNORECASE)

# Matches a plain rate/number (may have commas): 307,767.0 | 169,738.8
_RATE_RE   = re.compile(r'^[\d,]+(?:\.\d+)?$')

# Matches a percentage string: 34.08%
_PCT_RE    = re.compile(r'^[\d.]+%$')

# Matches "amount <whitespace including tab> rate" on one line
# e.g. "280.76m \t 176,653.6"
_INLINE_AMOUNT_RATE_RE = re.compile(
    r'^([\d,]+(?:\.\d+)?[kmb]?)\s+([,\d]+(?:\.\d+)?)\s*$',
    re.IGNORECASE
)

_SECTION_SKIP = {
    'Name', 'Amount', 'DPS', 'HPS',
    'Damage Done By Source', 'Healing Done By Source',
    'Damage Taken By Ability',
}


def _is_player_name(token):
    """
    Return True if the token looks like a player name rather than a
    number / percentage / keyword.
    """
    if not token or token in _SECTION_SKIP:
        return False
    if _PCT_RE.match(token):
        return False
    if _AMOUNT_RE.match(token):
        return False
    if _RATE_RE.match(token):
        return False
    # Must contain at least one letter
    if not re.search(r'[A-Za-z\u0400-\u04FF\u00C0-\u024F]', token):
        return False
    return True


def _parse_stat_section(lines, start, end):
    """
    Parse one stat section (damage done OR healing done) from `lines[start:end]`.
    Returns dict: { player_name: {'amount': str, 'dps_hps': str} }

    Handles all three line formats described above.
    """
    result = {}
    if start is None:
        return result

    # Collect non-empty stripped lines in the slice
    sec = [ln.strip() for ln in lines[start:end] if ln.strip()]

    i = 0
    while i < len(sec):
        token = sec[i]

        if token in _SECTION_SKIP:
            i += 1
            continue

        if not _is_player_name(token):
            i += 1
            continue

        name = token

        # ── peek at next line(s) ──────────────────────────────────────────

        next1 = sec[i + 1] if i + 1 < len(sec) else ''
        next2 = sec[i + 2] if i + 2 < len(sec) else ''
        next3 = sec[i + 3] if i + 3 < len(sec) else ''

        # Format A: Name / pct% / amount / rate   (4 tokens)
        if _PCT_RE.match(next1) and _AMOUNT_RE.match(next2):
            result[name] = {'amount': next2, 'dps_hps': next3}
            i += 4
            continue

        # Format C: Name / "amount\trate"   (inline, 2 tokens total)
        inline = _INLINE_AMOUNT_RATE_RE.match(next1)
        if inline and not _PCT_RE.match(next1):
            # Make sure the first capture is actually an amount (has suffix OR
            # is not just digits — distinguishes "280.76m" from a plain rate)
            candidate_amount = inline.group(1)
            candidate_rate   = inline.group(2)
            # Accept if it has a known suffix OR if next2 is a name/skip/empty
            # (meaning there's no separate rate line following)
            has_suffix = bool(re.search(r'[kmb]$', candidate_amount, re.IGNORECASE))
            next2_is_not_rate = (not _RATE_RE.match(next2)
                                 or not next2
                                 or next2 in _SECTION_SKIP
                                 or _is_player_name(next2))
            if has_suffix or next2_is_not_rate:
                result[name] = {'amount': candidate_amount, 'dps_hps': candidate_rate}
                i += 2
                continue

        # Format B: Name / amount / rate   (3 tokens, separate lines)
        if _AMOUNT_RE.match(next1) and _RATE_RE.match(next2):
            result[name] = {'amount': next1, 'dps_hps': next2}
            i += 3
            continue

        # Couldn't match — skip this token
        i += 1

    return result


def parse_damage_healing_flexible(text):
    """
    Locate 'Damage Done By Source', 'Healing Done By Source', and
    'Damage Taken By Ability' section boundaries, then delegate to
    _parse_stat_section for each block.
    """
    lines = text.split('\n')

    dmg_start = heal_start = dmg_taken_start = None
    for idx, line in enumerate(lines):
        s = line.strip()
        if   s == 'Damage Done By Source'   and dmg_start      is None:
            dmg_start      = idx
        elif s == 'Healing Done By Source'  and heal_start     is None:
            heal_start     = idx
        elif s == 'Damage Taken By Ability' and dmg_taken_start is None:
            dmg_taken_start = idx
            break  # nothing useful below this for our purposes

    damage_done  = _parse_stat_section(lines, dmg_start,  heal_start)
    healing_done = _parse_stat_section(lines, heal_start, dmg_taken_start)
    return damage_done, healing_done


# ---------------------------------------------------------------------------
# Regex fallback (compact single-line table formats)
# ---------------------------------------------------------------------------

def parse_damage_healing_tables(text):
    damage_done  = {}
    healing_done = {}

    name_g     = r'(' + NAME_CHARS + r'+)'
    amount_pat = r'([\d,]+(?:\.\d+)?[kmb]?)'
    rate_pat   = r'([\d,]+(?:\.\d+)?)'

    dmg_sec  = re.search(r'Damage Done By Source.*?(?=Healing Done By Source)',   text, re.DOTALL)
    heal_sec = re.search(r'Healing Done By Source.*?(?=Damage Taken By Ability)', text, re.DOTALL)

    if dmg_sec:
        for e in re.findall(
            name_g + r'\s*\n\s*[\d.]+%\s*\n\s*' + amount_pat + r'\s*\n?\s*' + rate_pat,
            dmg_sec.group(0), re.MULTILINE | re.IGNORECASE
        ):
            damage_done[e[0].strip()] = {'amount': e[1].strip(), 'dps_hps': e[2].strip()}

    if heal_sec:
        for e in re.findall(
            name_g + r'\s*\n\s*[\d.]+%\s*\n\s*' + amount_pat + r'\s*\n?\s*' + rate_pat,
            heal_sec.group(0), re.MULTILINE | re.IGNORECASE
        ):
            healing_done[e[0].strip()] = {'amount': e[1].strip(), 'dps_hps': e[2].strip()}

    return damage_done, healing_done


# ---------------------------------------------------------------------------
# DPS percentage
# ---------------------------------------------------------------------------

def compute_dps_percentages(damage_done):
    totals      = {name: amount_to_float(v['amount']) for name, v in damage_done.items()}
    grand_total = sum(totals.values())
    if grand_total == 0:
        return {name: '' for name in damage_done}
    return {
        name: f"{(amt / grand_total * 100):.2f}%"
        for name, amt in totals.items()
    }


# ---------------------------------------------------------------------------
# IsScoreUpgrade logic
# ---------------------------------------------------------------------------

def compute_score_upgrades(rows):
    """
    For each row (sorted oldest→newest), determine IsScoreUpgrade.

    A row is a score upgrade (1) when:
      - completion == 1, AND
      - this (dungeon, key_level) combination has never been completed before
        OR has been completed before but only at a strictly lower key level
        (i.e. this is the new personal best for that dungeon).

    The column also records how many attempts have been made on that dungeon
    since the last score upgrade on that dungeon. If it's not an upgrade, 0.

    State tracked per dungeon:
      best_completed_level  : highest key level completed so far (int or None)
      last_upgrade_index    : index in `rows` of the last score upgrade
      attempt_count         : total attempts on this dungeon (all key levels)
      upgrade_attempt_count : attempts since last upgrade (reset on upgrade)
    """
    # Work oldest→newest (rows are currently sorted newest→oldest)
    chronological = list(reversed(rows))

    # Per-dungeon state
    state = {}   # dungeon_name -> dict

    result = []  # (original_row_index, value)  — we'll map back later

    for _, row in enumerate(chronological):
        dungeon    = row.get('dungeon', '').strip()
        key_level  = row.get('key_level', '').strip()
        completion = row.get('completion', '')

        if not dungeon or not key_level:
            result.append('')
            continue

        try:
            kl = int(key_level)
        except ValueError:
            result.append('')
            continue

        if dungeon not in state:
            state[dungeon] = {
                'best_level':         None,   # highest completed key level
                'attempts_since_upg': 0,      # attempts since last upgrade
            }

        st = state[dungeon]
        st['attempts_since_upg'] += 1

        is_upgrade = False
        if completion == 1:
            if st['best_level'] is None or kl > st['best_level']:
                is_upgrade = True

        if is_upgrade:
            value = st['attempts_since_upg']   # includes this attempt
            st['best_level']         = kl
            st['attempts_since_upg'] = 0       # reset counter after upgrade
        else:
            value = 0

        result.append(value)

    # result is in chronological order; reverse to match rows (newest→oldest)
    result.reverse()
    return result


# ---------------------------------------------------------------------------
# CSV builder
# ---------------------------------------------------------------------------

def count_deaths(deaths, player_name):
    return sum(1 for d in deaths if d['player'] == player_name)


def build_csv_row(data):
    row = {}
    row['uploader']   = data.get('uploader',  '')
    row['dungeon']    = data.get('dungeon',   '')
    row['key_level']  = data.get('key_level', '')
    row['duration']   = data.get('duration',  '')
    row['timestamp']  = data.get('timestamp', '')
    row['completion'] = data.get('completion', '')
    # IsScoreUpgrade filled in later by compute_score_upgrades
    row['IsScoreUpgrade'] = ''

    deaths       = data.get('deaths', [])
    players      = data.get('players', [])
    damage_done  = data.get('damage_done',  {})
    healing_done = data.get('healing_done', {})
    dps_pcts     = data.get('dps_pcts',     {})

    # Build lookup: player name -> "Spec Class" string
    player_label = {}
    for p in players:
        label = f"{p.get('spec', '')} {p.get('class', '')}".strip()
        player_label[p['name']] = label if label else p['name']
    for i in range(1, 21):
        if i <= len(deaths):
            d = deaths[i - 1]
            label = player_label.get(d['player'], d['player'])
            row[f'death{i}'] = f"{label}: {d['killing_blow']} ({d['timestamp']})"
        else:
            row[f'death{i}'] = ''

    for idx in range(1, 6):
        px = f'player{idx}_'
        if idx <= len(players):
            p    = players[idx - 1]
            name = p['name']
            row[px + 'name']           = name
            row[px + 'class']          = p.get('class', '')
            row[px + 'spec']           = p.get('spec',  '')
            row[px + 'ilvl']           = p.get('ilvl',  '')
            row[px + 'role']           = p.get('role',  '')
            di = damage_done.get(name,  {})
            hi = healing_done.get(name, {})
            raw_amount = di.get('amount', '')
            duration_secs = duration_to_seconds(data.get('duration', ''))
            if raw_amount and duration_secs > 0:
                total_dmg = amount_to_float(raw_amount)
                avg_dps   = total_dmg / duration_secs
                if avg_dps >= 1_000_000:
                    avg_str = f"{avg_dps / 1_000_000:.1f}m"
                elif avg_dps >= 1_000:
                    avg_str = f"{avg_dps / 1_000:.1f}k"
                else:
                    avg_str = f"{avg_dps:.1f}"
                row[px + 'damage_amount'] = f"{raw_amount} ({avg_str})"
            else:
                row[px + 'damage_amount'] = raw_amount
            row[px + 'dps']            = di.get('dps_hps', '')
            row[px + 'damage_pct']     = dps_pcts.get(name, '')
            raw_heal = hi.get('amount', '')
            if raw_heal and duration_secs > 0:
                total_heal = amount_to_float(raw_heal)
                avg_hps    = total_heal / duration_secs
                if avg_hps >= 1_000_000:
                    avg_str = f"{avg_hps / 1_000_000:.1f}m"
                elif avg_hps >= 1_000:
                    avg_str = f"{avg_hps / 1_000:.1f}k"
                else:
                    avg_str = f"{avg_hps:.1f}"
                row[px + 'healing_amount'] = f"{raw_heal} ({avg_str})"
            else:
                row[px + 'healing_amount'] = raw_heal
            row[px + 'hps']            = hi.get('dps_hps', '')
            row[px + 'deaths']         = count_deaths(deaths, name)
            row[px + 'potions']        = p.get('potions',      '0')
            row[px + 'healthstones']   = p.get('healthstones', '0')
        else:
            for col in [
                'name', 'class', 'spec', 'ilvl', 'role',
                'damage_amount', 'dps', 'damage_pct',
                'healing_amount', 'hps',
                'deaths', 'potions', 'healthstones',
            ]:
                row[px + col] = ''

    return row


def get_csv_headers():
    h = ['uploader', 'dungeon', 'key_level', 'duration', 'timestamp',
         'completion', 'IsScoreUpgrade']
    for i in range(1, 21):
        h.append(f'death{i}')
    for idx in range(1, 6):
        px = f'player{idx}_'
        for col in [
            'name', 'class', 'spec', 'ilvl', 'role',
            'damage_amount', 'dps', 'damage_pct',
            'healing_amount', 'hps',
            'deaths', 'potions', 'healthstones',
        ]:
            h.append(px + col)
    return h


# ---------------------------------------------------------------------------
# File processing
# ---------------------------------------------------------------------------

def process_file(filepath):
    SHOW_DEBUG_PRINTS = False

    with open(filepath, 'r', encoding='utf-8') as f:
        text = f.read()

    if SHOW_DEBUG_PRINTS:
        print(f"\n{'='*60}")
        print(f"Processing: {filepath}")

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
    data['dps_pcts']     = compute_dps_percentages(dmg)
    data['completion']   = estimate_completion(
        data.get('dungeon', ''),
        data.get('duration', ''),
        data.get('deaths', []),
    )

    # ── debug ─────────────────────────────────────────────────
    if SHOW_DEBUG_PRINTS:
        print(f"  Uploader   : {data.get('uploader')}")
        print(f"  Log date   : {data.get('log_date')}")
        print(f"  Dungeon    : {data.get('dungeon')}")
        print(f"  Key Level  : {data.get('key_level')}")
        print(f"  Duration   : {data.get('duration')}")
        print(f"  Timestamp  : {data.get('timestamp')}")
        print(f"  Completion : {data.get('completion')}")
        print(f"  Players    : {len(data.get('players', []))}")
        for p in data.get('players', []):
            print(f"    - {p['name']} ({p['class']} {p['spec']}, "
                f"{p['ilvl']} ilvl, Role={p['role']}, "
                f"Pot={p['potions']}, HS={p['healthstones']})")
        print(f"  Deaths     : {len(data.get('deaths', []))}")
        for d in data.get('deaths', []):
            print(f"    - {d['player']} by {d['killing_blow']} at {d['timestamp']}")
        print(f"  DMG entries : {len(dmg)}")
        for nm, v in dmg.items():
            pct = data['dps_pcts'].get(nm, '?')
            print(f"    {nm}: {v}  →  group DMG% = {pct}")
        print(f"  Heal entries: {len(heal)}")
        for nm, v in heal.items():
            print(f"    {nm}: {v}")

    return build_csv_row(data)


# ---------------------------------------------------------------------------
# Log data validation
# ---------------------------------------------------------------------------


def compute_log_fingerprint(data):
    """
    Compute a fingerprint from the damage, healing, and death data.
    This is used to detect files where a previous run's stats were
    not replaced before the page was saved.
    """
    parts = []

    for name, entry in sorted(data.get('damage_done', {}).items()):
        parts.append(f"dmg:{name}:{entry.get('amount', '')}:{entry.get('dps_hps', '')}")

    for name, entry in sorted(data.get('healing_done', {}).items()):
        parts.append(f"heal:{name}:{entry.get('amount', '')}:{entry.get('dps_hps', '')}")

    for d in data.get('deaths', []):
        parts.append(f"death:{d.get('player', '')}:{d.get('killing_blow', '')}:{d.get('timestamp', '')}")

    combined = '|'.join(parts)
    return hashlib.md5(combined.encode('utf-8')).hexdigest()


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

    # Sort newest→oldest for the output CSV
    rows.sort(key=lambda r: get_sort_dt(r.get('timestamp', '')), reverse=True)

    # Compute IsScoreUpgrade (needs chronological awareness)
    upgrade_values = compute_score_upgrades(rows)
    for row, val in zip(rows, upgrade_values):
        row['IsScoreUpgrade'] = val

    headers = get_csv_headers()
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n{'='*60}")
    print(f"Written {len(rows)} rows to {OUTPUT_FILE}")

    print_milestone_summary(OUTPUT_FILE)


def score_to_avg_keylevel(total_score: float) -> float:
    """
    Given a player's total raider.io M+ score, return the average key level
    they timed across 8 dungeons, using the model:
        S(k) = 15k + 185  (for k >= 12, all affixes unlocked)

    Per-dungeon score = total_score / 8
    Solving for k: k = (per_dungeon_score - 185) / 15
    """
    per_dungeon = total_score / 8
    return (per_dungeon - 185) / 15


def print_milestone_summary(csv_file, player_scores=None):
    """
    Print a formatted milestone summary.

    Parameters
    ----------
    csv_file : str
        Path to the summary CSV produced by this script.
    player_scores : dict | None
        Optional { player_name: float } of current raider.io scores.
        When supplied, each player line includes their current score.
        Pass None (default) to omit scores.
    """

    # ── configuration ─────────────────────────────────────────────────────────
    ANONYMOUS   = False          # Set True to anonymise all player names
    MY_NAME     = "Powerpegging" # Your own character name (always shown as "Me")
    # ──────────────────────────────────────────────────────────────────────────

    if not os.path.isfile(csv_file):
        print(f"No file '{csv_file}' found.")
        return

    with open(csv_file, 'r', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))

    # Sort chronologically (oldest first)
    rows.sort(key=lambda r: get_sort_dt(r.get('timestamp', '')))

    # Anonymisation map  { real_name -> "anonXXX" }
    anon_map     = {}
    anon_counter = [0]  # mutable so the helper can increment it

    def get_anon_name(real_name):
        """Return the anonymised label for real_name."""
        if real_name == MY_NAME:
            return "Me"
        if real_name not in anon_map:
            anon_counter[0] += 1
            anon_map[real_name] = f"anon{anon_counter[0]:03d}"
        return anon_map[real_name]

    # Per-dungeon state
    state      = {}
    milestones = []

    for row in rows:
        dungeon    = row.get('dungeon',    '').strip()
        key_level  = row.get('key_level',  '').strip()
        timestamp  = row.get('timestamp',  '').strip()
        completion = row.get('completion', '').strip()
        duration   = row.get('duration',   '').strip()

        if not dungeon or not key_level:
            continue
        try:
            kl = int(key_level)
        except ValueError:
            continue

        if dungeon not in state:
            state[dungeon] = {
                'best_level':   None,
                'attempts':     0,
                'target_level': kl,
            }

        st = state[dungeon]

        if kl > st['target_level']:
            st['target_level'] = kl
            st['attempts']     = 0

        if kl < st['target_level']:
            continue

        st['attempts'] += 1

        if completion == '1' and (st['best_level'] is None or kl > st['best_level']):
            date_part    = timestamp[:10]
            attempt_word = 'attempt' if st['attempts'] == 1 else 'attempts'

            # ── time under timer ──────────────────────────────────────────
            timer_secs    = get_dungeon_timer(dungeon)
            duration_secs = duration_to_seconds(duration)
            if timer_secs and duration_secs:
                under_secs = timer_secs - duration_secs
                under_min  = under_secs // 60
                under_sec  = under_secs % 60
                under_str  = f"{under_min}min {under_sec:02d}sec time remaining"
            else:
                under_str  = "timer unknown"

            # ── header line – build with fixed-width fields so | aligns ──
            # "2026-04-24  " = 12 chars (date + 2 spaces)
            # dungeon name varies; key level "+ XX" varies; pad dungeon+key
            # We use a generous fixed width for the dungeon+key portion.
            # Longest dungeon name in the data: "The Seat of the Triumvirate" = 27
            # "+XX" = 3, space = 1  →  27+1+3 = 31 chars for dungeon+key block
            # attempts "(XX attempts)" longest = 14 chars
            # We'll just left-justify the dungeon+key in 32 chars.
            dungeon_key   = f"{dungeon} +{kl}"
            attempts_str  = f"({st['attempts']} {attempt_word})"

            # ── team composition ──────────────────────────────────────────
            # Column widths:
            #   name      : 20
            #   spec_cls  : 28  (bracket included)
            #   ilvl_col  : 8
            #   dps_col   : 10
            COL_NAME     = 20
            COL_SPEC_CLS = 28
            COL_ILVL     = 8
            COL_DPS      = 10

            team = []
            for idx in range(1, 6):
                px   = f'player{idx}_'
                name = row.get(px + 'name', '').strip()
                if not name:
                    continue

                spec  = row.get(px + 'spec',  '').strip()
                cls   = row.get(px + 'class', '').strip()
                ilvl  = row.get(px + 'ilvl',  '').strip()

                # Always show DPS regardless of role
                dmg_raw = row.get(px + 'damage_amount', '').strip()
                m = re.search(r'\(([^)]+)\)$', dmg_raw)
                avg_dps_str = f"{m.group(1)} dps" if m else ''

                # RIO score — always shown; use ???? if missing/zero
                score_str = 'rio ????'
                if player_scores is not None:
                    score_val = player_scores.get(name)
                    if score_val is not None and score_val != '':
                        try:
                            val = float(score_val)
                            if val > 0:
                                resil = score_to_avg_keylevel(val)
                                score_str = f"rio {val:.0f} (resil {resil:.1f})"
                        except (ValueError, TypeError):
                            pass

                # Anonymise if requested
                display_name = get_anon_name(name) if ANONYMOUS else name

                spec_cls = f"{spec} {cls}".strip() if (spec or cls) else '?'

                # Build padded columns
                name_col     = display_name.ljust(COL_NAME)
                spec_cls_col = f"[{spec_cls}]".ljust(COL_SPEC_CLS)
                ilvl_col     = (f"ilvl {ilvl}" if ilvl else '').ljust(COL_ILVL)
                dps_col      = avg_dps_str.ljust(COL_DPS) if avg_dps_str else ''.ljust(COL_DPS)

                line = f"    {name_col}  |  {spec_cls_col}  |  {ilvl_col}  |  {dps_col}  |  {score_str}"

                team.append(line)

            milestones.append({
                'date':          date_part,
                'dungeon':       dungeon,
                'dungeon_key':   dungeon_key,
                'attempts_str':  attempts_str,
                'kl':            kl,
                'attempts':      st['attempts'],
                'sort_dt':       get_sort_dt(timestamp),
                'attempt_word':  attempt_word,
                'under_str':     under_str,
                'team':          team,
            })

            st['best_level']   = kl
            st['target_level'] = kl + 1
            st['attempts']     = 0

    milestones.sort(key=lambda m: m['sort_dt'])

    # ── compute column widths for the header line ─────────────────────────────
    # "DATE  DUNGEON +KL  (X attempts)  —  Xmin XXsec time remaining"
    # We want the "—" to line up, so pad dungeon_key and attempts_str.
    max_dk  = max((len(m['dungeon_key'])  for m in milestones), default=0)
    max_att = max((len(m['attempts_str']) for m in milestones), default=0)

    print(f"\n{'='*60}")
    print("Mythic+ Milestone Summary")
    print('='*60)

    for m in milestones:
        dk_padded  = m['dungeon_key'].ljust(max_dk)
        att_padded = m['attempts_str'].ljust(max_att)
        print(
            f"\n{m['date']}  {dk_padded}  {att_padded}  —  {m['under_str']}"
        )
        for player_line in m['team']:
            print(player_line)

    print(f"\n{'='*60}")
    print(f"Total milestones: {len(milestones)}")
    print('='*60)



if __name__ == '__main__':
    main()
import json
import os
import requests
import re
import sys
import time
from datetime import datetime, timezone, timedelta
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor


class ConfigStuff:
    CACHE_IS_ENABLED = True  # Toggle caching functionality
    CACHE_TIMEOUT = 900  # 15 minutes written in seconds

    DEFAULT_REGION = "eu"
    OUTPUT_TABLE_LENGTH = 15
    OUTPUT_TABLE_ROLE_COUNT = 3
    OUTPUT_FILENAME = "output.txt"


class RatingPercentiles:
    CURRENT_SEASON = "season-tww-3"

    EU_DATA = {
        "season-sl-3": [3725.93, 3351.98, 2933.10, 2512.82, 1754.21],
        "season-sl-4": [3693.66, 3361.23, 2795.61, 2314.00, 1778.17],
        "season-df-1": [3430.90, 3200.94, 2793.27, 2552.71, 2246.54],
        "season-df-2": [3286.38, 2747.23, 1630.64,  955.61,  594.25],
        "season-df-3": [3720.74, 3377.09, 2881.04, 2523.69, 1976.22],
        "season-df-4": [3693.66, 3361.23, 2795.61, 2314.00, 1778.17],
        "season-tww-1":[3484.38, 3116.14, 2706.42, 2478.34, 2094.09],
        "season-tww-2":[3822.42, 3485.32, 3054.69, 2762.28, 2459.56],
    }

    US_DATA = {
        "season-sl-3": [3679.14, 3261.03, 2782.82, 2354.53, 1496.42],
        "season-sl-4": [3087.26, 2687.73, 2235.23, 2004.81, 1235.54],
        "season-df-1": [3378.85, 3083.75, 2722.84, 2434.62, 1990.45],
        "season-df-2": [3225.12, 2729.99, 1568.99,  904.15,  543.28],
        "season-df-3": [3688.18, 3287.81, 2788.99, 2342.54, 1682.42],
        "season-df-4": [3653.96, 3282.98, 2712.16, 2191.64, 1628.00],
        "season-tww-1":[3458.76, 3049.82, 2655.29, 2347.67, 1962.75],
        "season-tww-2":[3804.80, 3425.40, 3024.84, 2672.00, 2274.17],
    }

    ROLE_DPS = "dps"
    ROLE_HEAL = "heal"
    ROLE_TANK = "tank"

class WowPlayerFormatter:

    @staticmethod
    def main_pipeline():
        raw_input = WowPlayerFormatter._read_input()
        players = list(set(WowPlayerFormatter._format_player_signups(raw_input)))
        WowPlayerLookup.fetch_rio_profiles_concurrently(players)
        analysis_results = WowPlayerAnalysis.analyze_player_data(players)
        WowPlayerDisplay.sort_and_display_top_players(analysis_results)

    @staticmethod
    def _read_input():
        try:
            with open("input.json", "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            print("Error: input.json file not found")
        except json.JSONDecodeError:
            print("Error: input.json contains invalid JSON")
        except Exception as e:
            print(f"An error occurred: {str(e)}")
        return {}

    @staticmethod
    def _format_player_signups(raw_input):
        players = []
        region = raw_input.get("region", ConfigStuff.DEFAULT_REGION)
        player_signups = list(raw_input.get("queue", []))  # queue=signups, group=partymembers
        while player_signups:
            player = player_signups.pop()
            if isinstance(player, list):
                player_signups.extend(player)
            elif isinstance(player, str):
                match = re.match(r"(\d+)-(.+?)-(.+)", player)
                if match:
                    role_num = int(match.group(1))
                    name, realm = match.group(2), match.group(3)
                    role = (
                        RatingPercentiles.ROLE_DPS if role_num == 1 else
                        RatingPercentiles.ROLE_HEAL if role_num in (2, 3) else
                        RatingPercentiles.ROLE_TANK if role_num in (4, 5, 6, 7) else
                        "nan"
                    )
                    players.append((role, name, realm, region))
        return players


class WowPlayerLookup:

    RIO_API_RUN_URL = "https://raider.io/api/v1/mythic-plus/run-details"
    RIO_API_PLAYER_URL = "https://raider.io/api/v1/characters/profile"
    RIO_API_PLAYER_FIELDS = [
        "gear", "progress", "raid_progression",
        "mythic_plus_scores_by_season:season-sl-3:season-sl-4:season-df-1:season-df-2:season-df-3:season-df-4:season-tww-1:season-tww-2:season-tww-3",
        "mythic_plus_best_runs:all", "mythic_plus_recent_runs:all"
    ]
    preferred_dungeon_order: set = set()  # Automatically find the 8 current-season dungeon names

    @staticmethod
    def fetch_rio_profiles_concurrently(players):
        print(f"Processing {len(players)} players...")
        os.makedirs("player_data", exist_ok=True)
        # Thread pool for concurrency
        with ThreadPoolExecutor(max_workers=69) as executor:  # 69=Nice
            future_to_player = {
                executor.submit(WowPlayerLookup._concurrency_player_lookup_task, region, name, realm, role):
                (i+1, name, realm, region) for i, (role, name, realm, region) in enumerate(players)
            }
            completed = 0
            for future in concurrent.futures.as_completed(future_to_player):
                completed += 1
                _, name, realm, region = future_to_player[future]
                sys.stdout.write(f"\r\033[K{completed} of {len(players)} players processed... ({name}-{realm}-{region})")
                sys.stdout.flush()
                result = future.result()
                if result and "Error" in result:
                    sys.stdout.write(f"\n{result}\n")
                    sys.stdout.write(f"\r\033[K{completed} of {len(players)} players processed... ({name}-{realm}-{region})")
                    sys.stdout.flush()
        print("\nAll players processed successfully!")

    @staticmethod
    def _concurrency_player_lookup_task(region, name, realm, role):
        file_path = f"player_data/rio_{region}_{name}-{realm}.json"
        if ConfigStuff.CACHE_IS_ENABLED and os.path.exists(file_path):
            if ConfigStuff.CACHE_TIMEOUT > time.time() - os.path.getmtime(file_path):
                return None
        params = {
            "region": region,
            "realm": realm,
            "name": name,
            "fields": ",".join(WowPlayerLookup.RIO_API_PLAYER_FIELDS),
        }
        try:
            response = requests.get(WowPlayerLookup.RIO_API_PLAYER_URL, params=params, timeout=10)
            if response.status_code != 200:
                return f"Failed to fetch data for {name}-{realm}-{region}: {response.status_code}"

            data = response.json()
            data["queued_role"] = role
            data["region"] = region
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return f"Success for {name}-{realm}-{region}"
        except Exception as e:
            return f"Error fetching data for {name}-{realm}-{region}: {str(e)}"


class WowPlayerAnalysis:

    @staticmethod
    def analyze_player_data(players):
        analysis_results = {}
        for _, name, realm, region in players:
            file_path = f"player_data/rio_{region}_{name}-{realm}.json"
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                # Raid completion analysis
                heroic_complete, mythic_complete = WowPlayerAnalysis._get_raid_completion_counts(data.get('raid_progression', {}))
                # Basic player info
                player_class = WowPlayerAnalysis._format_class_name(data.get('class', ''))
                player_racial_and_gender = WowPlayerAnalysis._format_racial_name_and_gender(data.get('gender', ''), data.get('race', ''))
                achievement_points = data.get('achievement_points', 0)
                queued_role = data.get('queued_role', '')
                item_level = WowPlayerAnalysis._get_item_level(data.get('gear'))
                gem_enchant_count = WowPlayerAnalysis._count_gems_and_enchants(data.get('gear'))
                # Season scores
                all_scores, role_scores, current_season_score = WowPlayerAnalysis._get_season_scores(data.get('mythic_plus_scores_by_season', []), queued_role)
                # Recent runs analysis
                in_time_runs, total_runs, highest_timed_key = WowPlayerAnalysis._analyze_recent_runs(data.get('mythic_plus_recent_runs', []))
                avg_recent_key = WowPlayerAnalysis._calculate_avg_recent_key(data.get('mythic_plus_recent_runs', []), highest_timed_key)
                # Best runs analysis
                dungeon_levels, best_runs, time_ratios, avg_time_ratio, keystone_run_ids, completion_range = WowPlayerAnalysis._analyze_best_runs(data.get('mythic_plus_best_runs', []))
                analysis_results[name] = {
                    'heroic_complete_count': heroic_complete,
                    'mythic_complete_count': mythic_complete,
                    'player_class': player_class,
                    'player_racial_and_gender': player_racial_and_gender,
                    'achievement_points': achievement_points,
                    'region': region,
                    'queued_role': queued_role,
                    'all_scores': all_scores,
                    'role_scores': role_scores,
                    'current_season_score': current_season_score,
                    'mythic_plus_scores_by_season': data.get('mythic_plus_scores_by_season', []),  # Add this line
                    'item_level': item_level,
                    'gem_enchant_count': gem_enchant_count,
                    'in_time_runs': in_time_runs,
                    'total_recent_runs': total_runs,
                    'highest_timed_key': highest_timed_key,
                    'avg_recent_key': avg_recent_key,
                    'dungeon_levels': dungeon_levels,
                    'best_runs': best_runs,
                    'time_ratios': time_ratios,
                    'avg_time_ratio': avg_time_ratio,
                    'keystone_run_ids': keystone_run_ids,
                    'completion_range': completion_range,
                }
            except Exception as e:
                print(f"Error analyzing player data in {file_path}: {str(e)}")
                return None
        return analysis_results

    @staticmethod
    def _get_raid_completion_counts(raid_progression):
        """Calculate heroic and mythic raid completion counts"""
        heroic_complete = mythic_complete = 0
        for raid_info in raid_progression.values():
            total_bosses = raid_info.get('total_bosses', 0)
            if raid_info.get('heroic_bosses_killed', 0) == total_bosses:
                heroic_complete += 1
            if raid_info.get('mythic_bosses_killed', 0) == total_bosses:
                mythic_complete += 1
        return heroic_complete, mythic_complete

    @staticmethod
    def _format_class_name(player_class):
        """Format player class name for display"""
        class_mapping = {"Demon Hunter": "D.Hunt", "Death Knight": "DK",
                         "Paladin": "Pala", "Warrior": "Warr",
                         "Warlock": "Lock"}
        return class_mapping.get(player_class, player_class)

    @staticmethod
    def _format_racial_name_and_gender(gender, race):
        """Format player class name for display"""
        gender_mapping = {"female": "F", "male": "M"}
        racial_mapping = {"Night Elf": "Nelf", "Blood Elf": "Belf"}
        return f"{gender_mapping.get(gender, gender)} {racial_mapping.get(race, race)}"

    @staticmethod
    def _get_item_level(gear_data):
        """Extract and validate item level from gear data"""
        if not gear_data:
            return 0
        raw_item_level = gear_data.get('item_level_equipped', 0)
        try:
            return round(float(raw_item_level)) if raw_item_level else 0
        except (ValueError, TypeError):
            return 0

    @staticmethod
    def _count_gems_and_enchants(gear_data):
        """Count total gems + enchants for neck, finger1, finger2"""
        if not gear_data or "items" not in gear_data:
            return "?"
        items = gear_data["items"]
        enchant_slots = ["mainhand", "chest", "legs", "back", "wrist", "feet", "finger1", "finger2"]
        gem_slots = ["neck", "finger1", "finger2"]
        total = 14
        for slot in enchant_slots:
            item = items.get(slot)
            if not item or not "enchant" in item:
                continue
            total -= 1
        for slot in gem_slots:
            item = items.get(slot)
            if not item:
                continue
            total -= len(item.get("gems", []))
        if total == 0:
            return "-"
        return total

    @staticmethod
    def _get_season_scores(mythic_plus_scores_by_season, queued_role):
        """Extract high scores (>=2000) for all roles and queued role, plus current season score"""
        all_scores, role_scores = [], []
        current_season_score = 0
        role_mapping = {RatingPercentiles.ROLE_DPS: 'dps', RatingPercentiles.ROLE_HEAL: 'healer', RatingPercentiles.ROLE_TANK: 'tank'}
        for season_data in mythic_plus_scores_by_season:
            scores = season_data.get('scores', {})
            season = season_data.get('season', '')
            # All scores
            all_score = scores.get('all', 0)
            if all_score >= 2000:
                all_scores.append(all_score)
            # Current season score
            if season == RatingPercentiles.CURRENT_SEASON:
                current_season_score = all_score
            # Role-specific scores
            role_key = role_mapping.get(queued_role)
            if role_key:
                role_score = scores.get(role_key, 0)
                if role_score >= 2000:
                    role_scores.append(role_score)
        return all_scores, role_scores, current_season_score

    @staticmethod
    def _analyze_recent_runs(recent_runs):
        """Analyze recent runs to calculate timing statistics"""
        # Find highest timed key
        highest_timed_key = max((run.get('mythic_level', 0) for run in recent_runs if
                                run.get('clear_time_ms', 0) <= run.get('par_time_ms', float('inf')) and
                                run.get('clear_time_ms', 0) > 0), default=0)
        min_key_level = max(2, highest_timed_key - 2)
        in_time_runs = total_runs_in_range = 0
        for run in recent_runs:
            key_level = run.get('mythic_level', 0)
            if key_level >= min_key_level:
                total_runs_in_range += 1
                clear_time = run.get('clear_time_ms', 0)
                par_time = run.get('par_time_ms', 0)
                if 0 < clear_time <= par_time:
                    in_time_runs += 1
        return in_time_runs, total_runs_in_range, highest_timed_key

    @staticmethod
    def _analyze_best_runs(best_runs_data):
        """Analyze best runs to get dungeon levels and time ratios"""
        keystone_run_ids = []
        best_runs = {}
        time_ratios = {}
        WowPlayerFormatter.preferred_dungeon_order = sorted(set(run.get("short_name") for run in best_runs_data))

        for run in best_runs_data:
            short_name = run.get('short_name', '')
            key_level = run.get('mythic_level', 0)
            clear_time = run.get('clear_time_ms', 0)
            par_time = run.get('par_time_ms', 0)
            keystone_run_id = run.get("keystone_run_id")
            if keystone_run_id:
                keystone_run_ids.append(keystone_run_id)
            # Calculate time ratio
            time_ratio = 0
            if 0 < clear_time <= par_time:
                time_ratio = (par_time - clear_time) / par_time
            # Update if better
            should_update = (
                short_name not in best_runs or
                key_level > best_runs[short_name] or
                (key_level == best_runs[short_name] and time_ratio > time_ratios.get(short_name, 0))
            )
            if should_update:
                best_runs[short_name] = key_level
                time_ratios[short_name] = time_ratio

        # Create ordered dungeon levels list
        dungeon_levels = [best_runs.get(dungeon, 0) for dungeon in WowPlayerFormatter.preferred_dungeon_order]
        # Add any additional dungeons
        additional_dungeons = sorted(d for d in best_runs.keys() if d not in WowPlayerFormatter.preferred_dungeon_order)
        dungeon_levels.extend(best_runs[dungeon] for dungeon in additional_dungeons)
        # Calculate average time ratio
        completed_dungeons = [d for d in time_ratios if time_ratios[d] > 0]
        avg_time_ratio = sum(time_ratios.values()) / len(completed_dungeons) if completed_dungeons else 0

        # Analyze completion dates
        completion_range = WowPlayerAnalysis._analyze_completion_dates(best_runs_data)

        return dungeon_levels, best_runs, time_ratios, avg_time_ratio, keystone_run_ids, completion_range

    @staticmethod
    def _calculate_avg_recent_key(recent_runs, highest_timed_key):
        """Calculate average key level for recent runs >= (highest_timed_key - 2)"""
        if highest_timed_key == 0:
            return 0
        min_key_level = max(2, highest_timed_key - 2)
        valid_runs = [run.get('mythic_level', 0) for run in recent_runs
                    if run.get('mythic_level', 0) >= min_key_level]
        return sum(valid_runs) / len(valid_runs) if valid_runs else 0

    @staticmethod
    def _analyze_completion_dates(best_runs_data):
        """Analyze completion dates to find newest and average time of timed runs"""
        timed_runs_dates = []
        current_time = datetime.now(timezone.utc)
        for run in best_runs_data:
            completed_at = run.get('completed_at')
            clear_time = run.get('clear_time_ms', 0)
            par_time = run.get('par_time_ms', 0)
            # Only consider timed runs (clear_time <= par_time and both > 0)
            if completed_at and 0 < clear_time <= par_time:
                try:
                    # Parse the ISO timestamp
                    completion_date = datetime.fromisoformat(completed_at.replace('Z', '+00:00'))
                    timed_runs_dates.append(completion_date)
                except ValueError:
                    continue
        if not timed_runs_dates:
            return "N/A"
        # Find newest run
        newest_date = max(timed_runs_dates)
        newest_diff = current_time - newest_date
        # Calculate average time
        total_time_diff = sum((current_time - date).total_seconds() for date in timed_runs_dates)
        avg_time_diff_seconds = total_time_diff / len(timed_runs_dates)
        # Create timedelta object for average
        avg_time_diff = timedelta(seconds=avg_time_diff_seconds)
        # Format time differences
        newest_str = WowPlayerAnalysis._format_time_difference(newest_diff)
        avg_str = WowPlayerAnalysis._format_time_difference(avg_time_diff)
        if newest_str == avg_str:
            return newest_str
        else:
            return f"{newest_str} / {avg_str}"

    @staticmethod
    def _format_time_difference(time_diff):
        """Format time difference into readable string"""
        total_seconds = int(time_diff.total_seconds())
        if total_seconds < 3600:  # Less than 1 hour
            minutes = total_seconds // 60
            return f"{minutes}min" if minutes > 0 else "now"
        elif total_seconds < 86400:  # Less than 1 day
            hours = total_seconds // 3600
            return f"{hours}h"
        else:  # Days or more
            days = total_seconds // 86400
            return f"{days}d"

class WowPlayerDisplay:

    @staticmethod
    def sort_and_display_top_players(player_data):
        # Sort all players by ranking criteria
        ranked_players = []
        for name, data in player_data.items():
            if not data:
                continue
            # Primary criterion: mythic completion
            primary = -data['mythic_complete_count']
            # Secondary criterion: best percentile (lower = better)
            percentile_str = WowPlayerDisplay._get_highest_score_percentile(
                data.get('mythic_plus_scores_by_season', []),
                data.get('region', ConfigStuff.DEFAULT_REGION)
            )
            secondary = WowPlayerDisplay._parse_percentile(percentile_str)
            ranked_players.append((name, data, (primary, secondary)))
        ranked_players.sort(key=lambda x: x[2])
        # Separate players by role
        tanks = [(i, name, data) for i, (name, data, _) in enumerate(ranked_players) if data['queued_role'] == RatingPercentiles.ROLE_TANK]
        healers = [(i, name, data) for i, (name, data, _) in enumerate(ranked_players) if data['queued_role'] == RatingPercentiles.ROLE_HEAL]
        dps = [(i, name, data) for i, (name, data, _) in enumerate(ranked_players) if data['queued_role'] == RatingPercentiles.ROLE_DPS]
        # Select top players ensuring role balance
        selected_players = []
        used_indices = set()
        # First, add players in ranking order until we have enough or run out
        for i, (name, data, _) in enumerate(ranked_players):
            if len(selected_players) < ConfigStuff.OUTPUT_TABLE_LENGTH:
                selected_players.append((i + 1, name, data))  # i+1 for 1-based ranking
                used_indices.add(i)
            else:
                break
        # Ensure minimum role counts using the helper function
        WowPlayerDisplay._ensure_role_balance(selected_players, used_indices, tanks, RatingPercentiles.ROLE_TANK, ConfigStuff.OUTPUT_TABLE_ROLE_COUNT)
        WowPlayerDisplay._ensure_role_balance(selected_players, used_indices, healers, RatingPercentiles.ROLE_HEAL, ConfigStuff.OUTPUT_TABLE_ROLE_COUNT)
        WowPlayerDisplay._ensure_role_balance(selected_players, used_indices, dps, RatingPercentiles.ROLE_DPS, ConfigStuff.OUTPUT_TABLE_ROLE_COUNT)
        # Sort selected players by their original ranking for display
        output_lines = []
        selected_players.sort(key=lambda x: x[0])
        if selected_players:
            # Create header
            dungeon_header = "(" + "-".join(d[:2].upper() for d in WowPlayerFormatter.preferred_dungeon_order) + ")"
            header = f"{'Rank':<4} {'Player':<15} {'Role':<6} {'Class':<7} {'Racial':<11} {'!':<3} {'iLvl':<5} {'CE':<4} {'AOTC':<4} {'KSM':<4} {'Best':<7} {'Score':<7} {'Time':<10} {'Recent':<12} {'Age':<9} {dungeon_header}"
            separator = "-" * len(header)
            output_lines.extend(["\nTop Players Ranking:", separator, header, separator])
            print("\nTop Players Ranking:")
            print(separator)
            print(header)
            print(separator)
            # Display players
            for _, (original_rank, name, data) in enumerate(selected_players, 1):
                # Get region from player data
                region = data.get('region', ConfigStuff.DEFAULT_REGION)
                # Format display elements
                formatted_highest = WowPlayerDisplay._get_highest_score_percentile(data.get('mythic_plus_scores_by_season', []), region)
                # Use current season score instead of average
                current_score = data.get('current_season_score', 0)
                formatted_current = WowPlayerDisplay._format_score(current_score) if current_score > 0 else "N/A"
                time_with_avg = WowPlayerDisplay._format_time_with_avg_key(data)
                recent_status = WowPlayerDisplay._format_recent_runs(data)
                dungeon_str = WowPlayerDisplay._format_dungeon_levels(data.get('dungeon_levels', []))
                player_line = (f"{original_rank:<4} {name:<15} {data['queued_role']:<6} {data['player_class']:<7} "
                            f"{data['player_racial_and_gender']:<11} {data['gem_enchant_count']:<3} {data['item_level']:<5} "
                            f"{data['mythic_complete_count']:<4} {data['heroic_complete_count']:<4} "
                            f"{len(data['all_scores']):<4} {formatted_highest:<7} {formatted_current:<7} "
                            f"{time_with_avg:<10} {recent_status:<12} {data.get('completion_range', 'N/A'):<9} {dungeon_str}")
                output_lines.append(player_line)
                print(player_line)
            legend = "Dungeon order: " + "/".join(WowPlayerFormatter.preferred_dungeon_order)
            output_lines.extend([separator, legend, separator])
            print(separator)
            print(legend)
            print(separator)
        else:
            no_data_msg = "No player data available for ranking."
            output_lines.append(no_data_msg)
            print(no_data_msg)
        # Save results
        with open(ConfigStuff.OUTPUT_FILENAME, "w", encoding="utf-8") as f:
            f.write("\n".join(output_lines))
        print(f"\nResults have been saved to {ConfigStuff.OUTPUT_FILENAME}")
        return selected_players

    @staticmethod
    def _format_score(score):
        """Format score in K format"""
        return f"{score/1000:.1f}k" if score >= 1000 else f"{score:.0f}"

    @staticmethod
    def _format_recent_runs(data):
        """Format recent runs display string with average key level"""
        total_runs = data.get('total_recent_runs', 0)
        if total_runs == 0:
            return "N/A"
        in_time = data.get('in_time_runs', 0)
        avg_key = data.get('avg_recent_key', 0)
        base_str = f"{in_time}/{total_runs}"
        if avg_key > 0:
            spaces = "  " if len(base_str) == 3 else " " if len(base_str) == 4 else ""
            return f"{base_str}{spaces}(+{avg_key:.0f})"
        return base_str

    @staticmethod
    def _format_dungeon_levels(dungeon_levels):
        """Format dungeon levels for display"""
        if not dungeon_levels:
            return "[-  -  -  -  -  -  -  -]"
        formatted_levels = []
        for level in dungeon_levels:
            if level == 0:
                formatted_levels.append("-  ")
            elif level <= 9:
                formatted_levels.append(f"{level}  ")
            else:
                formatted_levels.append(f"{level} ")
        return "[" + "".join(formatted_levels).rstrip() + "]"

    @staticmethod
    def _score_to_percentile(score, season, region):
        if region == "eu" and season in RatingPercentiles.EU_DATA:
            threshholds = RatingPercentiles.EU_DATA[season]
        elif region == "us" and season in RatingPercentiles.US_DATA:
            threshholds = RatingPercentiles.US_DATA[season]
        else:
            print(f"Region '{region}' or season '{season}' is missing for RatingPercentile data.")
            threshholds = [0.0, 0.0, 0.0, 0.0, 0.0]
        # Known percentile thresholds (percentile in %, score)
        data = [(0.1,threshholds[0]),
                (1,  threshholds[1]),
                (10, threshholds[2]),
                (25, threshholds[3]),
                (40, threshholds[4])]
        # If score is higher than top 0.1%
        if score >= data[0][1]:
            return data[0][0]
        # If score is lower than worst known point (40%)
        if score <= data[-1][1]:
            return data[-1][0]
        # Otherwise interpolate between two nearest known points
        for (p1, s1), (p2, s2) in zip(data, data[1:]):
            if s1 >= score >= s2:
                # Linear interpolation
                frac = (score - s2) / (s1 - s2)
                percentile = p2 + frac * (p1 - p2)
                return percentile
        raise ValueError("Score out of bounds")

    @staticmethod
    def _get_highest_score_percentile(mythic_plus_scores_by_season, region):
        """Get percentile for highest score across all seasons except current"""
        if not mythic_plus_scores_by_season:
            return "-"
        # Filter out current season and get all scores from previous seasons
        previous_season_scores = []
        for season_data in mythic_plus_scores_by_season:
            season = season_data.get('season', '')
            if season != RatingPercentiles.CURRENT_SEASON:
                all_score = season_data.get('scores', {}).get('all', 0)
                if all_score >= 2000:  # Only consider scores >= 2000
                    previous_season_scores.append((all_score, season))
        # If no previous season data, return "-"
        if not previous_season_scores:
            return "-"
        # Get the highest score and its season
        highest_score, best_season = max(previous_season_scores, key=lambda x: x[0])
        # Calculate percentile for that specific season
        try:
            percentile = WowPlayerDisplay._score_to_percentile(highest_score, best_season, region)
            return f"{percentile:.1f}%"
        except ValueError:
            return "-"

    @staticmethod
    def _parse_percentile(percentile_str):
        """Convert percentile string like '0.1%' into float. Lower = better."""
        if not percentile_str or percentile_str == "-":
            return float("inf")  # treat missing as worst
        try:
            return float(percentile_str.strip("%"))
        except ValueError:
            return float("inf")

    @staticmethod
    def _ensure_role_balance(selected_players, used_indices, role_players, target_role, min_count):
        """Ensure minimum role count of 3 or more players per role in the final top15 table"""
        current_count = sum(1 for _, _, data in selected_players if data['queued_role'] == target_role)
        if current_count >= min_count:
            return
        available_players = [(i, name, data) for i, name, data in role_players if i not in used_indices]
        needed = min(min_count - current_count, len(available_players))
        if needed > 0:
            # Remove lowest ranked players that aren't of the target role
            players_to_remove = []
            for rank, name, data in reversed(selected_players):
                if data['queued_role'] != target_role and len(players_to_remove) < needed:
                    players_to_remove.append((rank, name, data))
            for player in players_to_remove:
                selected_players.remove(player)
                used_indices.remove(player[0] - 1)  # Convert back to 0-based
            # Add the best available players of target role
            for orig_idx, name, data in available_players[:needed]:
                selected_players.append((orig_idx + 1, name, data))
                used_indices.add(orig_idx)

    @staticmethod
    def _format_time_with_avg_key(data):
        """Format time ratio with average key level in parentheses"""
        time_ratio_pct = f"{data.get('avg_time_ratio', 0):.0%}" if data.get('avg_time_ratio', 0) > 0 else "N/A"
        # Calculate average key level
        dungeon_levels = data.get('dungeon_levels', [])
        if dungeon_levels and any(level > 0 for level in dungeon_levels):
            # Only count dungeons with level > 0
            valid_levels = [level for level in dungeon_levels if level > 0]
            avg_key = sum(valid_levels) / len(valid_levels) if valid_levels else 0
            if avg_key > 0:
                if data.get('avg_time_ratio', 0) >= 0.095:
                    return f"{time_ratio_pct}(+{avg_key:.0f})"
                return f"{time_ratio_pct} (+{avg_key:.0f})"
        return time_ratio_pct

if __name__ == "__main__":
    WowPlayerFormatter.main_pipeline()
import os
import csv
import time
import requests
from datetime import datetime
from outscraper import ApiClient

# ----------------------------
# CONFIG
# ----------------------------

OUTSCRAPER_API_KEY = os.getenv("OUTSCRAPER_API_KEY")
if not OUTSCRAPER_API_KEY:
    raise RuntimeError("Run: set OUTSCRAPER_API_KEY=YOUR_KEY")

GOOGLE_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
if not GOOGLE_API_KEY:
    raise RuntimeError("Run: set GOOGLE_MAPS_API_KEY=YOUR_KEY")

MAX_PER_CITY = 3

OUT_SUMMARY = "us_chipotle_popular_times_summary.csv"
OUT_HOURLY  = "us_chipotle_popular_times_hourly.csv"

# ----------------------------
# MAJOR US CITIES
# ----------------------------

CITIES = [
    ("New York",         40.7128,  -74.0060),
    ("Los Angeles",      34.0522, -118.2437),
    ("Chicago",          41.8781,  -87.6298),
    ("Houston",          29.7604,  -95.3698),
    ("Phoenix",          33.4484, -112.0740),
    ("Philadelphia",     39.9526,  -75.1652),
    ("San Antonio",      29.4241,  -98.4936),
    ("San Diego",        32.7157, -117.1611),
    ("Dallas",           32.7767,  -96.7970),
    ("San Jose",         37.3382, -121.8863),
    ("Austin",           30.2672,  -97.7431),
    ("Jacksonville",     30.3322,  -81.6557),
    ("Fort Worth",       32.7555,  -97.3308),
    ("Columbus",         39.9612,  -82.9988),
    ("Charlotte",        35.2271,  -80.8431),
    ("Indianapolis",     39.7684,  -86.1581),
    ("San Francisco",    37.7749, -122.4194),
    ("Seattle",          47.6062, -122.3321),
    ("Denver",           39.7392, -104.9903),
    ("Nashville",        36.1627,  -86.7816),
    ("Oklahoma City",    35.4676,  -97.5164),
    ("El Paso",          31.7619, -106.4850),
    ("Boston",           42.3601,  -71.0589),
    ("Portland",         45.5051, -122.6750),
    ("Las Vegas",        36.1699, -115.1398),
    ("Memphis",          35.1495,  -90.0490),
    ("Louisville",       38.2527,  -85.7585),
    ("Baltimore",        39.2904,  -76.6122),
    ("Milwaukee",        43.0389,  -87.9065),
    ("Albuquerque",      35.0844, -106.6504),
    ("Tucson",           32.2226, -110.9747),
    ("Fresno",           36.7378, -119.7871),
    ("Sacramento",       38.5816, -121.4944),
    ("Mesa",             33.4152, -111.8315),
    ("Kansas City",      39.0997,  -94.5786),
    ("Atlanta",          33.7490,  -84.3880),
    ("Omaha",            41.2565,  -95.9345),
    ("Colorado Springs", 38.8339, -104.8214),
    ("Raleigh",          35.7796,  -78.6382),
    ("Minneapolis",      44.9778,  -93.2650),
    ("Tampa",            27.9506,  -82.4572),
    ("New Orleans",      29.9511,  -90.0715),
    ("Miami",            25.7617,  -80.1918),
    ("Boca Raton",       26.3683,  -80.1289),
    ("Orlando",          28.5383,  -81.3792),
    ("Pittsburgh",       40.4406,  -79.9959),
    ("Cincinnati",       39.1031,  -84.5120),
    ("Cleveland",        41.4993,  -81.6944),
    ("St. Louis",        38.6270,  -90.1994),
    ("Detroit",          42.3314,  -83.0458),
]


# ----------------------------
# STEP 1: Find Chipotles via Google Places
# ----------------------------

def find_chipotles_in_city(city, lat, lng):
    places = {}
    url = (
        f"https://maps.googleapis.com/maps/api/place/nearbysearch/json"
        f"?location={lat},{lng}&radius=15000"
        f"&keyword=Chipotle+Mexican+Grill&type=restaurant&key={GOOGLE_API_KEY}"
    )
    while url and len(places) < MAX_PER_CITY:
        resp = requests.get(url).json()
        for p in resp.get("results", []):
            if len(places) >= MAX_PER_CITY:
                break
            pid  = p.get("place_id")
            name = p.get("name", "")
            addr = p.get("vicinity", "")
            if pid and "chipotle" in name.lower():
                places[pid] = {"name": name, "address": addr, "city": city}
        token = resp.get("next_page_token")
        if token and len(places) < MAX_PER_CITY:
            time.sleep(2)
            url = (
                f"https://maps.googleapis.com/maps/api/place/nearbysearch/json"
                f"?pagetoken={token}&key={GOOGLE_API_KEY}"
            )
        else:
            url = None
    return places


def find_all_chipotles():
    print(f"Step 1: Finding up to {MAX_PER_CITY} Chipotles in {len(CITIES)} cities...")
    all_places = {}
    for i, (city, lat, lng) in enumerate(CITIES, start=1):
        print(f"  [{i}/{len(CITIES)}] {city}...")
        city_places = find_chipotles_in_city(city, lat, lng)
        all_places.update(city_places)
        print(f"           -> {len(city_places)} found")
        time.sleep(0.5)
    print(f"\n  Total unique locations: {len(all_places)}\n")
    return all_places


# ----------------------------
# STEP 2: Fetch Popular Times via Outscraper
# ----------------------------

def fetch_popular_times(places):
    print(f"Step 2: Fetching Popular Times for {len(places)} locations...")
    client     = ApiClient(api_key=OUTSCRAPER_API_KEY)
    items      = list(places.items())
    batch_size = 20
    all_results = []

    for i in range(0, len(items), batch_size):
        batch     = items[i:i + batch_size]
        batch_num = i // batch_size + 1
        total     = -(-len(items) // batch_size)
        print(f"  Batch {batch_num}/{total}...")

        queries = [f"{info['address']}, {info['city']}" for _, info in batch]
        try:
            results = client.google_maps_search(
                queries,
                fields=["name", "full_address", "place_id",
                        "popular_times", "current_popularity", "rating"],
                language="en",
                region="us",
            )
            all_results.append((batch, results))
        except Exception as e:
            print(f"  [WARN] Batch error: {e}")
        time.sleep(1)

    return all_results


# ----------------------------
# STEP 3: Parse Outscraper format & Save
# ----------------------------

def parse_popular_times(pop_list):
    """
    Outscraper returns popular_times as a list of dicts:
    [{'day': 1, 'day_text': 'Monday', 'popular_times': [{'hour': 10, 'percentage': 45, ...}]}, ...]
    """
    parsed = {}
    if not pop_list or not isinstance(pop_list, list):
        return parsed

    for day_entry in pop_list:
        day_name = day_entry.get("day_text", "")
        hours    = day_entry.get("popular_times", [])
        if day_name and hours:
            parsed[day_name] = {
                str(h["hour"]): h["percentage"]
                for h in hours if "hour" in h and "percentage" in h
            }
    return parsed


def process_and_save(places, all_results):
    summary_rows = []
    hourly_rows  = []
    timestamp    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for batch, results in all_results:
        for idx, result_list in enumerate(results):
            city = batch[idx][1]["city"] if idx < len(batch) else ""

            for place in result_list:
                pid     = place.get("place_id", "")
                name    = place.get("name", "")
                address = place.get("full_address", "")
                current = place.get("current_popularity")
                rating  = place.get("rating")
                pop     = parse_popular_times(place.get("popular_times"))

                peak_day   = None
                peak_hour  = None
                peak_value = 0

                days = ["Monday", "Tuesday", "Wednesday",
                        "Thursday", "Friday", "Saturday", "Sunday"]

                for day in days:
                    day_data = pop.get(day, {})
                    for hour_str, val in day_data.items():
                        try:
                            h   = int(hour_str)
                            lbl = f"{h % 12 or 12}{'am' if h < 12 else 'pm'}"
                            hourly_rows.append({
                                "timestamp":    timestamp,
                                "city":         city,
                                "place_id":     pid,
                                "name":         name,
                                "address":      address,
                                "day":          day,
                                "hour_24":      h,
                                "hour_label":   lbl,
                                "busyness_pct": val,
                                "title":        "",
                            })
                            if val > peak_value:
                                peak_value = val
                                peak_day   = day
                                peak_hour  = lbl
                        except Exception:
                            pass

                summary_rows.append({
                    "timestamp":          timestamp,
                    "city":               city,
                    "place_id":           pid,
                    "name":               name,
                    "address":            address,
                    "rating":             rating,
                    "peak_day":           peak_day,
                    "peak_hour":          peak_hour,
                    "peak_busyness_pct":  peak_value,
                    "current_popularity": current,
                    "has_data":           "Yes" if peak_value > 0 else "No",
                })

    # Save CSVs
    if summary_rows:
        with open(OUT_SUMMARY, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=summary_rows[0].keys())
            writer.writeheader()
            writer.writerows(summary_rows)
        print(f"\nSaved: {OUT_SUMMARY} ({len(summary_rows)} locations)")

    if hourly_rows:
        with open(OUT_HOURLY, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=hourly_rows[0].keys())
            writer.writeheader()
            writer.writerows(hourly_rows)
        print(f"Saved: {OUT_HOURLY} ({len(hourly_rows)} rows)")

    # Print top 20
    has_data    = [r for r in summary_rows if r["has_data"] == "Yes"]
    no_data     = [r for r in summary_rows if r["has_data"] == "No"]
    sorted_rows = sorted(has_data, key=lambda x: x["peak_busyness_pct"], reverse=True)

    print(f"\n{'='*100}")
    print(f"  TOP 20 BUSIEST CHIPOTLES ACROSS THE US")
    print(f"  With data: {len(has_data)} | No data: {len(no_data)}")
    print(f"{'='*100}")
    print(f"\n{'City':<18} {'Address':<40} {'Peak Day':<12} {'Peak Hour':<10} {'Peak %'}")
    print("-" * 95)
    for row in sorted_rows[:20]:
        print(
            f"{str(row['city']):<18} {str(row['address'])[:38]:<40} "
            f"{str(row['peak_day']):<12} {str(row['peak_hour']):<10} "
            f"{row['peak_busyness_pct']}%"
        )


# ----------------------------
# MAIN
# ----------------------------

def main():
    print("US Chipotle Popular Times Scanner")
    print(f"Scanning {MAX_PER_CITY} per city x {len(CITIES)} cities")
    print("=" * 55)

    all_places  = find_all_chipotles()
    if not all_places:
        print("No locations found.")
        return

    all_results = fetch_popular_times(all_places)
    process_and_save(all_places, all_results)
    print("\nDone!")


if __name__ == "__main__":
    main()

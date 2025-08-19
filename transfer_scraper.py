# -*- coding: utf-8 -*-
"""
Eintracht Frankfurt transfer scraper (2016+)
- Player, Age, Position, Nationality, From/To Club, Fee, Fee Type, Fee (EUR), Market Value, Direction, Season
- SQLite tablosuna kaydeder
"""

import re, sqlite3, time
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd

BASE = "https://www.transfermarkt.com/eintracht-frankfurt/transfers/verein/24"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9,tr;q=0.8",
}

START_SEASON = 2016
CURRENT_YEAR = datetime.now().year
END_SEASON = max(START_SEASON, min(CURRENT_YEAR + 1, 2026))

def clean_text(s):
    return re.sub(r"\s+", " ", s).strip() if s else ""

def parse_money(s):
    if not s: return None
    m = re.search(r"€\s*([\d.,]+)\s*([mk])?", s, re.I)
    if m:
        val = float(m.group(1).replace(",", "."))
        suf = (m.group(2) or "").lower()
        if suf=="m": val*=1_000_000
        if suf=="k": val*=1_000
        return val
    if re.search(r"€\s*0\b", s): return 0.0
    return None

def parse_fee(text):
    if not text: return None, "other", ""
    t = text.lower()
    if "free" in t: return 0.0, "free", text
    if "end of loan" in t: return 0.0, "loan_return", text
    if "loan" in t:
        amt = parse_money(t)
        return amt, "loan", text
    if "undisclosed" in t or "n/a" in t or t=="-":
        return None, "undisclosed", text
    amt = parse_money(t)
    return (amt, "transfer", text) if amt is not None else (None, "other", text)

def parse_nationality(td):
    if not td: return ""
    nats = [img.get("title") or img.get("alt") for img in td.find_all("img") if img.get("title") or img.get("alt")]
    return ", ".join([clean_text(n) for n in nats])

def extract_rows(table, direction, season_label):
    rows_data = []
    if not table: return rows_data
    for tr in table.find_all("tr", class_=re.compile("^(odd|even)$")):
        player = age = position = nationality = from_club = to_club = fee_text = market_value = ""

        # Player & Position
        player_cell = tr.select_one("td.hauptlink")
        if player_cell:
            player_a = player_cell.find("a", href=re.compile("/profil/spieler/"))
            player = clean_text(player_a.text) if player_a else ""
            # Position, genellikle ikinci tr içinde
            pos_tr = player_cell.find_next("tr")
            if pos_tr:
                position = clean_text(pos_tr.get_text())

        # Age
        age_td = tr.select_one("td.zentriert")
        age = clean_text(age_td.text) if age_td else ""

        # Nationality
        nat_td = tr.select_one("td.zentriert img.flaggenrahmen")
        if nat_td:
            nationality = nat_td.get("title") or nat_td.get("alt") or ""
            nationality = clean_text(nationality)

        # From / To Club
        club_td = tr.find_all("td")[-3] if len(tr.find_all("td")) >= 3 else None
        if club_td:
            club_img = club_td.find("img")
            club_a = club_td.find("a", href=re.compile(r"/startseite/verein/"))
            club_name = clean_text(club_img.get("title")) if club_img else (clean_text(club_a.text) if club_a else "")
            if direction=="Arrival": from_club = club_name
            else: to_club = club_name

        # Fee / Market Value
        fee_td = tr.find_all("td")[-1] if len(tr.find_all("td")) >= 1 else None
        fee_text = clean_text(fee_td.text) if fee_td else ""
        fee_amount, fee_type, fee_raw = parse_fee(fee_text)

        rows_data.append({
            "Season": season_label,
            "Direction": direction,
            "Player": player,
            "Age": age,
            "Position": position,
            "Nationality": nationality,
            "From Club": from_club,
            "To Club": to_club,
            "Fee (raw)": fee_raw,
            "Fee Type": fee_type,
            "Fee (EUR)": fee_amount,
            "Market Value": fee_text
        })
    return rows_data

def scrape_season(season_id):
    url = f"{BASE}/saison_id/{season_id}"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, "html.parser")
    data = []
    for header in soup.select("h2.content-box-headline"):
        title = header.get_text(strip=True).lower()
        container = header.find_next("div", class_="responsive-table")
        table = container.find("table", class_="items") if container else None
        if "arrivals" in title:
            data.extend(extract_rows(table,"Arrival",f"{season_id}/{str(season_id+1)[-2:]}"))
        elif "departures" in title:
            data.extend(extract_rows(table,"Departure",f"{season_id}/{str(season_id+1)[-2:]}"))
    return data

def main():
    all_rows=[]
    for yr in range(START_SEASON, END_SEASON+1):
        try:
            print(f"Scraping season {yr}/{str(yr+1)[-2:]} ...")
            season_rows = scrape_season(yr)
            print(f" -> {len(season_rows)} rows")
            all_rows.extend(season_rows)
            time.sleep(1)
        except Exception as e:
            print(f"Error scraping season {yr}: {e}")

    if not all_rows:
        print("No data scraped.")
        return

    df = pd.DataFrame(all_rows)
    cols=["Season","Direction","Player","Age","Position","Nationality",
          "From Club","To Club","Fee (raw)","Fee Type","Fee (EUR)","Market Value"]
    df = df.reindex(columns=cols)

    conn = sqlite3.connect("frankfurt_transfers_full.sqlite")
    df.to_sql("transfers",conn,if_exists="replace",index=False)
    conn.close()
    print(f"Done ✅ Total rows: {len(df)}")
    print("SQLite saved.")

if __name__=="__main__":
    main()

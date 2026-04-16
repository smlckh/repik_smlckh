import pandas as pd
from bs4 import BeautifulSoup
import requests
import anthropic
import base64
import time
import os
from datetime import datetime
from dotenv import load_dotenv
import gspread
from gspread_dataframe import set_with_dataframe


gc = gspread.service_account(filename="google_credentials.json")
sh = gc.open("bazos_scraper_table")

# ==================== NASTAVENÍ Z GOOGLE SHEETS ====================
setup_ws = sh.worksheet("nastaveni")
df_kriteria = pd.DataFrame(setup_ws.get_all_records())
prompt_ws = sh.worksheet("prompt")
prompt_template = prompt_ws.acell("A1").value
top_n = 10

# ==================== COOKIES ====================
session = requests.Session()
session.cookies.set("bid", "85664201", domain=".bazos.cz")
session.cookies.set("bkod", "HFLSNZ1ZXJ", domain=".bazos.cz")
session.cookies.set("bjmeno", "Samuel", domain=".bazos.cz")
session.cookies.set("btelefon", "777006248", domain=".bazos.cz")
session.cookies.set("testcookie", "ano", domain=".bazos.cz")

# ==================== API ====================
os.environ["ANTHROPIC_API_KEY"] = os.getenv("ANTHROPIC_API_KEY")
client = anthropic.Anthropic()

# ==================== 1. SCRAPING (LOOP) ====================
vsechny = []

for idx, row in df_kriteria.iterrows():
    hledat = row["hledat"]
    rubriky = row["rubriky"]
    lokalita = row["lokalita"]
    humkreis = row["humkreis"]
    cenaod = row["cena od"]
    cenado = row["cena do"]
    klicova_slova_text = row["klicova_slova"]
    anti_slova = row["anti_slova"]

    print(f"\n=== Hledám: {hledat} ===")

    for i in range(0, 100, 20):
        url = f"https://nabytek.bazos.cz/{i}/?hledat={hledat}&rubriky={rubriky}&hlokalita={lokalita}&humkreis={humkreis}&cenaod={cenaod}&cenado={cenado}"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "lxml")
        inzeraty = soup.find_all("div", class_="inzeraty")

        for div in inzeraty:
            nadpis = div.find("h2", class_="nadpis")
            if nadpis:
                zaznam = {
                    "hledany_vyraz": hledat,
                    "klicova_slova": klicova_slova_text,
                    "anti_slova": anti_slova,
                    "titulek": nadpis.text,
                    "url": nadpis.find("a")["href"],
                    "cena": div.find("div", class_="inzeratycena").text,
                    "lokalita": div.find("div", class_="inzeratylok").get_text(separator="|"),
                    "popis": div.find("div", class_="popis").text,
                    "foto": div.find("img")["src"],
                    "datum_pridani": div.find("span", class_="velikost10").text
                }
                vsechny.append(zaznam)

        print(f"  Stránka {i // 20 + 1}: {len(inzeraty)} inzerátů")
        time.sleep(1)

df = pd.DataFrame(vsechny)
print(f"\nCelkem staženo: {len(df)} inzerátů")

# ==================== 2. ČIŠTĚNÍ DAT ====================
df["cena_cislo"] = df["cena"].str.replace("Kč", "").str.replace(" ", "").str.strip()
df["cena_cislo"] = pd.to_numeric(df["cena_cislo"], errors="coerce")
df["mesto"] = df["lokalita"].str.split("|").str[0].str.strip()
df["psc"] = df["lokalita"].str.split("|").str[1].str.strip()
df["full_url"] = "https://nabytek.bazos.cz" + df["url"]

df = df.drop_duplicates(subset="full_url")
print(f"Po deduplikaci: {len(df)} inzerátů")

# ==================== DEDUPLIKACE OD POSLEDNÍHO ====================
vysledky_ws = sh.worksheet("vysledky")
df_old = pd.DataFrame(vysledky_ws.get_all_records())

seen_urls = []
if not df_old.empty and "full_url" in df_old.columns:
    seen_urls = df_old["full_url"].tolist()

df = df[~df["full_url"].isin(seen_urls)]
print(f"Nových inzerátů: {len(df)}")

if len(df) == 0:
    print("Žádné nové inzeráty. Končím.")
else:
    # ==================== 3. DETAILY ====================
    detail_data = []

    for url in df["full_url"]:
        response = session.get(url)
        soup = BeautifulSoup(response.text, "lxml")

        popis_elem = soup.find("div", class_="popisdetail")
        popis = popis_elem.text if popis_elem else None

        fotky = soup.find_all("img", class_="carousel-cell-image")
        foto_urls = [img["data-flickity-lazyload"] for img in fotky]

        detail_data.append({
            "full_url": url,
            "popis_detail": popis,
            "foto_urls": foto_urls,
            "pocet_fotek": len(foto_urls)
        })

        print(f"Detail: {url[-40:]}")
        time.sleep(1)

    df_detail = pd.DataFrame(detail_data)
    df = df.merge(df_detail, on="full_url", how="left")

    # ==================== 4. RULE-BASED FILTR ====================
    def pocet_shod(row):
        text = (row["titulek"] + " " + str(row["popis_detail"])).lower()
        slova = row["klicova_slova"].split(", ")
        anti = row["anti_slova"].split(", ")
        score = sum(1 for slovo in slova if slovo in text)
        score -= sum(1 for slovo in anti if slovo in text)
        return score

    df["keyword_score"] = df.apply(pocet_shod, axis=1)
    df = df.sort_values("keyword_score", ascending=False)
    print(f"\nKeyword scoring hotovo. Top 5:")
    print(df[["titulek", "cena_cislo", "keyword_score", "hledany_vyraz"]].head())

    # ==================== 5. AI SCORING ====================
    top = df.head(top_n)
    scores = []

    for idx, row in top.iterrows():
        foto_list = row["foto_urls"][:2] if row["foto_urls"] else []

        content = []
        for foto_url in foto_list:
            img_response = requests.get(foto_url)
            img_base64 = base64.b64encode(img_response.content).decode("utf-8")
            content.append({"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": img_base64}})

        content.append({"type": "text", "text": prompt_template.format(
    titulek=row['titulek'],
    cena=row['cena'],
    mesto=row['mesto'],
    popis=row['popis_detail']
    )})

        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            messages=[{"role": "user", "content": content}]
        )

        ai_text = message.content[0].text
        scores.append({"full_url": row["full_url"], "ai_hodnoceni": ai_text})
        print(f"\n{row['titulek']} | {row['cena']}")
        print(ai_text)
        time.sleep(1)

    df_scores = pd.DataFrame(scores)
    df_scores["score"] = df_scores["ai_hodnoceni"].str.extract(r"SCORE: (\d+)/10")
    df_scores["score"] = df_scores["score"].astype(int)

    df_ranked = df.merge(df_scores, on="full_url", how="left")
    df_ranked = df_ranked.sort_values("score", ascending=False)

    # ==================== 6. TELEFONY (TOP 5) ====================
    for idx, row in df_ranked.head(5).iterrows():
        response = session.get(row["full_url"])
        soup = BeautifulSoup(response.text, "lxml")

        tel_span = soup.find("span", class_="teldetail")
        if tel_span and tel_span.get("onclick"):
            onclick = tel_span["onclick"]
            params = onclick.split("'")[3]
            idi = params.split("&")[0].split("=")[1]
            idphone = params.split("&")[1].split("=")[1]
            tel_response = session.post(
                "https://nabytek.bazos.cz/ad-phone.php",
                data={"idi": idi, "idphone": idphone, "teloverit": "777006248"}
            )
            tel_soup = BeautifulSoup(tel_response.text, "lxml")
            tel_elem = tel_soup.find("a", class_="teldetail")
            tel = tel_elem.text if tel_elem else "N/A"
        else:
            tel = "N/A"

        df_ranked.loc[idx, "telefon"] = tel
        print(f"Tel pro {row['titulek']}: {tel}")
        time.sleep(3)

    # ==================== 7. EXPORT DO GOOGLE SHEETS ====================
    if not df_old.empty:
        df_final = pd.concat([df_old, df_ranked])
        df_final = df_final.drop_duplicates(subset="full_url", keep="last")
        df_final = df_final.sort_values("score", ascending=False)
    else:
        df_final = df_ranked

    # Convert lists to strings for Google Sheets compatibility
    if "foto_urls" in df_final.columns:
        df_final["foto_urls"] = df_final["foto_urls"].astype(str)

    vysledky_ws.clear()
    set_with_dataframe(vysledky_ws, df_final)

    # ==================== 8. LOG ====================
    with open("bazos_log.txt", "a") as f:
        f.write(f"{datetime.now()} | Inzerátů: {len(df)} | Scored: {len(df_scores)} | Top score: {df_ranked['score'].max()}\n")
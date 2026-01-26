import re
import time
from urllib.parse import urlparse, parse_qs, urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}
# регулярки для IMO и MMSi
RE_IMO = re.compile(r"\bIMO\b\D*(\d{7})\b", re.IGNORECASE)
RE_MMSI = re.compile(r"\bMMSI\b\D*(\d{9})\b", re.IGNORECASE)

def soup_from_response(resp: requests.Response) -> BeautifulSoup:
    return BeautifulSoup(resp.text, "lxml")


def parse_input_link(raw_url: str) -> tuple[str, str | None]:
    raw_url = (raw_url or "").strip()
    if not raw_url:
        return "", None

    p = urlparse(raw_url)
    base_url = f"{p.scheme}://{p.netloc}{p.path}" if p.scheme and p.netloc else raw_url

    qs = parse_qs(p.query, keep_blank_values=True)
    name = qs.get("name", [None])[0]

    return base_url, name


def fetch_search_page(session: requests.Session, base_url: str, name: str | None) -> BeautifulSoup:
    if name is None:
        resp = session.get(base_url, headers=HEADERS, timeout=30)
    else:
        resp = session.get(base_url, params={"name": name}, headers=HEADERS, timeout=30)

    resp.raise_for_status()
    return soup_from_response(resp)


def extract_vessel_links_from_search(soup: BeautifulSoup) -> list[str]:
    links = set()

    for a in soup.select('a[href]'):
        href = a.get("href", "")
        if "/vessels/details/" in href:
            links.add(urljoin("https://www.vesselfinder.com", href.split("?")[0]))

    return sorted(links)


def fetch_soup(session: requests.Session, url: str) -> BeautifulSoup:
    resp = session.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return soup_from_response(resp)


def extract_name_imo_mmsi_type_from_vessel_page(soup: BeautifulSoup):
    name = None
    h1 = soup.find("h1")
    if h1:
        name = h1.get_text(strip=True)

    text = soup.get_text(separator="\n", strip=True)

    # IMO MMSI
    imo = RE_IMO.search(text)
    imo = imo.group(1) if imo else None

    mmsi = RE_MMSI.search(text)
    mmsi = mmsi.group(1) if mmsi else None


    vtype = None

    # ищем таблицу для типа судна
    reg_table = soup.find("table")
    if reg_table:
        rows = reg_table.find_all("tr")
        for row in rows:
            tds = row.find_all("td", class_=re.compile("tpl"))
            if len(tds) >= 2:
                label = tds[0].get_text(strip=True)
                value = tds[1].get_text(strip=True)

                if label == "Тип":
                    vtype = value
                    break

    # если не нашли тип в таблице
    if not vtype:
        pat = re.compile(r"Тип\s*</td[^>]*>\s*<td[^>]*>([^<]+?)</td>", re.IGNORECASE | re.DOTALL)
        m = pat.search(str(soup))
        if m:
            vtype = m.group(1).strip()

    return name, imo, mmsi, vtype


def main():
    inp_path = "Links.xlsx"
    out_path = "result.xlsx"

    df = pd.read_excel(inp_path)
    if "Ссылка" not in df.columns:
        raise ValueError(f"Нет колонки 'Ссылка'. Колонки в файле: {list(df.columns)}")

    urls = df["Ссылка"].dropna().astype(str).tolist()

    rows = []
    processed = 0
    skipped = 0
    errors = 0

    with requests.Session() as session:
        for i, raw_url in enumerate(urls, start=1):
            raw_url = (raw_url or "").strip()
            if not raw_url:
                continue

            try:
                base_url, name = parse_input_link(raw_url)

                print(f"[{i}/{len(urls)}] Проверяю: {raw_url[:80]}...", end=" ", flush=True)

                search_soup = fetch_search_page(session, base_url, name)
                vessel_links = extract_vessel_links_from_search(search_soup)

                if len(vessel_links) != 1:
                    print(f"ПРОПУСК ({len(vessel_links)} судов)")
                    skipped += 1
                    continue

                vessel_url = vessel_links[0]
                vessel_soup = fetch_soup(session, vessel_url)

                vname, imo, mmsi, vtype = extract_name_imo_mmsi_type_from_vessel_page(vessel_soup)

                rows.append({
                    "Название": vname,
                    "IMO": imo,
                    "MMSI": mmsi,
                    "Тип": vtype,
                })

                print("OK")
                processed += 1
                time.sleep(0.5)

            except Exception as e:
                print(f"ОШИБКА: {str(e)[:120]}")
                errors += 1
                continue

    result = pd.DataFrame(rows, columns=["Название", "IMO", "MMSI", "Тип"])
    result.to_excel(out_path, index=False, engine="openpyxl")

    print("Готово!")
    print(f"  Обработано (взяты данные): {processed}")
    print(f"  Пропущено (не 1 судно):   {skipped}")
    print(f"  Ошибок:                   {errors}")
    print(f"  Результат:                {out_path}")


if __name__ == "__main__":
    main()

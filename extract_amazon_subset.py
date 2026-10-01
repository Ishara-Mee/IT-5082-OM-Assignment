"""Cut a small subset from the full Amazon Last Mile 2021 data.

Put route_data.json, package_data.json and travel_times.json in data/raw/amazon_full/,
then run:  python tools/extract_amazon_subset.py
Output: data/raw/amazon_subset.json (12 high-quality routes from one station).
"""
import json
from pathlib import Path
import ijson

RAW = Path(__file__).resolve().parents[1] / "data" / "raw"
SRC, DST = RAW / "amazon_full", RAW / "amazon_subset.json"


class NaNFile:
    """The Amazon files contain bare NaN, which is not valid JSON. Turn it into null while streaming."""
    def __init__(self, f):
        self.f, self.carry, self.eof = f, b"", False

    def read(self, n=-1):
        if n == 0 or self.eof:                       # ijson calls read(0) to detect bytes vs str
            return b""
        chunk = self.f.read(n if n > 0 else 1 << 20)
        buf = (self.carry + chunk).replace(b"NaN", b"null")
        if chunk:                                    # a NaN may be split across two chunks, so hold back a partial one
            hold = 2 if buf.endswith(b"Na") else 1 if buf.endswith(b"N") else 0
            self.carry, buf = buf[len(buf) - hold:], buf[:len(buf) - hold]
        else:
            self.carry, self.eof = b"", True
        return buf


def plain(v):
    """ijson gives Decimal for floats; convert to normal floats."""
    return json.loads(json.dumps(v, default=float))


def stream(name):
    """Yield (route_id, value) pairs from one big file without loading it all."""
    path = next(SRC.rglob(name))
    with open(path, "rb") as f:
        yield from ijson.kvitems(NaNFile(f), "")


def main(n_routes=12):
    # 1) pick routes: first n_routes high-quality routes of one station
    route_data, station = {}, None
    for rid, rd in stream("route_data.json"):
        rd = plain(rd)
        if rd.get("route_score") != "High":
            continue
        station = station or rd["station_code"]
        if rd["station_code"] == station:
            route_data[rid] = rd
        if len(route_data) >= n_routes:
            break
    out = {"station": station, "route_data": route_data}

    # 2) pull the same routes from the other two files
    for key in ("package_data", "travel_times"):
        out[key] = {}
        for rid, val in stream(f"{key}.json"):
            if rid in route_data:
                out[key][rid] = plain(val)
                if len(out[key]) == len(route_data):
                    break
        print(f"{key}: {len(out[key])} routes")

    DST.write_text(json.dumps(out))
    print(f"saved {DST} ({DST.stat().st_size / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()

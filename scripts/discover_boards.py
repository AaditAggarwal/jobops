"""Board discovery: find and verify new ATS board tokens for the watchlist.

Three candidate sources, all public and documented:

* ``repos``   - new-grad/internship listing repos (SimplifyJobs et al.). Their
  job URLs contain *real* board tokens, so this is extraction, not guessing,
  and it is by far the highest-yield source.
* ``yc``      - the Y Combinator company directory (yc-oss public JSON mirror),
  filtered to companies currently hiring. YC has no ATS field, so tokens are
  guessed from the company slug/name and then verified live.
* ``curated`` - hand-listed majors that rarely appear in listing repos.

Every candidate is probed against the live ATS API before it is written; a
token that does not resolve never enters the watchlist. Probes run one
sequential stream per provider (the polite-client rule), with results cached in
``data/.discovery_cache.json`` so re-runs cost nothing.

Usage:
    uv run python scripts/discover_boards.py --sources repos,yc,curated
    uv run python scripts/discover_boards.py --dry-run          # report only
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Iterable

import httpx
import yaml

from jobops.ingest.board_probe import ATS_ORDER, probe
from jobops.ingest.common import (
    TAIL_WATCHLIST_PATH,
    WATCHLIST_PATH,
    load_watchlist,
    polite_client,
)

CACHE_PATH = Path(__file__).resolve().parents[1] / "data" / ".discovery_cache.json"
CACHE_FLUSH_EVERY = 100  # probes between cache writes, so a long run is resumable

YC_HIRING_API = "https://yc-oss.github.io/api/companies/hiring.json"

# Listing repos whose job URLs carry real board tokens. Raw file URLs only:
# no GitHub API quota, no scraping.
LISTING_SOURCES = [
    "https://raw.githubusercontent.com/SimplifyJobs/New-Grad-Positions/dev/.github/scripts/listings.json",
    "https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships/dev/.github/scripts/listings.json",
    "https://raw.githubusercontent.com/vanshb03/Summer2026-Internships/dev/README.md",
    "https://raw.githubusercontent.com/speedyapply/2026-SWE-College-Jobs/main/README.md",
    "https://raw.githubusercontent.com/speedyapply/2026-AI-College-Jobs/main/README.md",
]

# Token patterns per ATS. Kept deliberately tight so a stray URL cannot inject
# a garbage token (probing still has the final say).
TOKEN_PATTERNS = {
    "greenhouse": [
        re.compile(r"(?:boards|job-boards)(?:\.eu)?\.greenhouse\.io/(?:embed/job_board\?for=)?([a-z0-9_-]{2,40})", re.I),
        re.compile(r"boards-api\.greenhouse\.io/v1/boards/([a-z0-9_-]{2,40})", re.I),
        re.compile(r"greenhouse\.io/embed/job_app\?for=([a-z0-9_-]{2,40})", re.I),
    ],
    "lever": [re.compile(r"jobs\.(?:eu\.)?lever\.co/([a-z0-9_.-]{2,40})", re.I)],
    "ashby": [
        re.compile(r"jobs\.ashbyhq\.com/([a-z0-9_.-]{2,40})", re.I),
        re.compile(r"ashbyhq\.com/posting-api/job-board/([a-z0-9_.-]{2,40})", re.I),
    ],
    "smartrecruiters": [
        re.compile(r"jobs\.smartrecruiters\.com/([A-Za-z0-9_-]{2,40})"),
        re.compile(r"careers\.smartrecruiters\.com/([A-Za-z0-9_-]{2,40})"),
    ],
}

# Path segments that are not company tokens.
TOKEN_STOPWORDS = {
    "embed", "job_board", "jobs", "api", "v1", "boards", "search", "www",
    "job", "careers", "company", "companies", "posting", "postings", "oauth",
}

# Curation: CLAUDE.md targeting rule - only pursue employers that can plausibly
# sponsor an F-1 -> H-1B. Defense/ITAR shops require citizenship; staffing and
# consulting mills flood the feed with recycled reqs.
EXCLUDE_PAT = re.compile(
    r"\b(defense|defence|aerospace.*defen|itar|clearance|classified|"
    r"staffing|recruit(ing|ment)|consult(anc\w*|ing|ants)|"
    r"talent ?(solutions|acquisition)|outsourc|manpower|placement services|"
    r"it ?services|(info|tele)tech|solutions? (inc|llc)|technologies (inc|llc)|"
    r"systems (inc|llc)|services (inc|llc)|group (inc|llc))\b",
    re.I,
)

# Tokens arrive as one squashed word ("NorthStarStaffingSolutions1"), so the
# exclusion regex only bites after camel case is split back into words.
CAMEL_SPLIT = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|[-_.]|(?<=[A-Z])(?=[A-Z][a-z])")


def words_in_token(token: str) -> str:
    """Turn an ATS token back into spaced words for curation matching (pure)."""
    return re.sub(r"\d+$", "", CAMEL_SPLIT.sub(" ", token)).strip()


EXCLUDE_TOKENS = {
    # Defense primes and clearance-gated shops. An F-1 student cannot hold the
    # clearance these roles require and the employer cannot sponsor into them,
    # so every application is wasted effort — this is a targeting rule, not a
    # judgement about the companies.
    "shieldai", "anduril", "palantirtechnologies", "raytheon", "lockheedmartin",
    "northropgrumman", "generaldynamics", "l3harris", "bae", "leidos", "saic",
    "boozallen", "mitre", "aerospacecorp", "draper", "sandia", "llnl", "mitll",
    "wyetechllc", "sosi", "systemstechnologyresearch", "freedomconsulting",
    "redhorsecorp", "pingwind", "peraton", "caci", "manteching", "nightwing",
    "twosix", "kbr", "amentum", "parsons", "jacobs", "battelle", "riverside",
    # Defense/space startups surfaced by discovery (2026-09-01 curation pass):
    # all ITAR-controlled and US-persons-only in practice.
    "saronic", "trueanomalyinc", "k2spacecorporation", "vast", "voyagertechnologiesinc",
    "allen-control-systems", "allencontrolsystems", "northwoodspace", "antares",
    "apex-technology-inc", "apextechnology", "hadrian-automation", "hadrianautomation",
    "skydio", "castelion", "ursamajor", "firestorm", "epirus", "cx2", "chaos-industries",
    "chaosindustries", "regent", "hermeus", "rocketlab", "firefly", "stoke-space",
    "stokespace", "abl", "relativityspace", "varda", "astranis", "impulse-space",
    "impulsespace", "picogrid", "scout-ai", "second-front", "secondfront",
    # Staffing, body shops, and outsourcing/consulting mills.
    "insightglobal", "roberthalf", "teksystems", "aerotek", "randstad", "adecco",
    "kforce", "collabera", "cognizant", "infosys", "wipro", "hcl", "tcs",
    "accenture", "deloitte", "capgemini", "mindtree", "ltimindtree", "virtusa",
    "altentechnologyusa", "innodatainc", "charlesriverassociates",
}

# Space and weapons work is ITAR-controlled almost without exception.
DEFENSE_HINT_PAT = re.compile(
    r"\b(space ?(corp|systems|technolog|force)|aerospace|orbital|spacecraft|"
    r"missile|munition|hypersonic|warfare|weapons|tactical systems|"
    r"national security|govtech|federal solutions)\b",
    re.I,
)

# Majors and strong mid-size employers that seldom show up in listing repos.
# Slug guesses only - each is verified live before it is written anywhere.
CURATED = """
stripe airbnb databricks snowflake datadog cloudflare coinbase robinhood plaid
brex ramp gusto rippling notion figma canva asana atlassian dropbox box twilio
mongodb elastic confluent hashicorp gitlab docker sentry launchdarkly postman
vercel netlify supabase render digitalocean fastly okta crowdstrike sentinelone
zscaler snyk wiz tenable rapid7 abnormalsecurity duolingo grammarly discord
reddit pinterest snap doordash instacart lyft uber airtable amplitude mixpanel
segment retool linear vanta drata sardine unit21 alloy checkr persona
scaleai anthropic openai perplexity cohere huggingface runwayml midjourney
character sierra harvey glean cursor anysphere together replicate modal
fireworksai baseten weightsandbiases langchain llamaindex pinecone weaviate
chroma qdrant neon planetscale cockroachlabs yugabyte timescale clickhouse
starburst dbtlabs fivetran airbyte hightouch census montecarlodata soda
temporal camunda prefect dagster astronomer bytewax redpanda materialize
stripeclimate affirm chime marqeta mercury column modern-treasury increase
wise revolut monzo n26 nubank remitly payoneer bill navan expensify brexhq
carta addepar betterment wealthfront ellevest publicholdings alpaca tradier
jane-street citadel two-sigma hudson-river-trading jump-trading drw imc
optiver akuna belvedere old-mission tower-research radix squarepoint
verition balyasny millennium point72 schonfeld qube xtx
nvidia amd arm qualcomm broadcom marvell astera cerebras groq sambanova
tenstorrent lightmatter psiquantum rigetti ionq quantinuum
waymo zoox nuro aurora applied-intuition motional cruise rivian lucidmotors
tesla joby archer boomsupersonic relativityspace varda astranis
verkada samsara flexport project44 gopuff faire whatnot mercari poshmark
klaviyo attentive braze iterable customerio sendbird courier knock
twelve-labs assemblyai deepgram elevenlabs suno udio synthesia heygen
sourcegraph replit codeium magic poolside augmentcode
benchling recursion insitro tempus color invitae flatiron oscar devoted
included alto ro hims cedar zocdoc headway spring-health lyra
ramp-financial deel-global oyster remotecom velocityglobal papaya
duolingo-eng khanacademy coursera udemy chegg quizlet outschool
roblox epicgames riotgames unity niantic scopely playco discord-gaming
figma-design canva-au miro mural pitch tome gamma beautifulai
stord flexe shipbob deliverr shipmonk convoy transfix
"""


def slug_candidates(name: str, slug: str = "") -> list[str]:
    """Token guesses for a company, most likely first (pure).

    ATS tokens are nearly always the company name squashed to lowercase, with
    or without separators: "Modern Treasury" -> moderntreasury / modern-treasury.
    """
    out: list[str] = []
    for base in (slug, name):
        if not base:
            continue
        low = re.sub(r"[^a-z0-9]+", " ", base.lower()).strip()
        if not low:
            continue
        for form in (low.replace(" ", ""), low.replace(" ", "-")):
            if 2 <= len(form) <= 40 and form not in out:
                out.append(form)
    return out


def extract_tokens(text: str) -> set[tuple[str, str]]:
    """Pull (ats, token) pairs out of any text containing ATS job URLs (pure)."""
    found: set[tuple[str, str]] = set()
    for ats, patterns in TOKEN_PATTERNS.items():
        for pat in patterns:
            for m in pat.finditer(text):
                token = m.group(1)
                if token.lower() in TOKEN_STOPWORDS:
                    continue
                # SmartRecruiters tokens are case-sensitive; the rest are not.
                found.add((ats, token if ats == "smartrecruiters" else token.lower()))
    return found


def is_excluded(name: str, token: str) -> bool:
    """Whether curation policy rejects a candidate outright (pure).

    The token itself is checked as well as the label: discovery mines tokens
    out of URLs, so the company name is often only present in the token
    ("USITSolutionsInc", "NorthStarStaffingSolutions1"). Session 2b found
    SmartRecruiters extraction to be ~80% staffing firms; this is the filter
    that keeps them out.
    """
    flat = token.lower().replace("-", "").replace(".", "")
    if flat in EXCLUDE_TOKENS or token.lower() in EXCLUDE_TOKENS:
        return True
    words = words_in_token(token)
    if DEFENSE_HINT_PAT.search(name) or DEFENSE_HINT_PAT.search(words):
        return True
    return bool(EXCLUDE_PAT.search(name) or EXCLUDE_PAT.search(words))


def us_relevant(location: str) -> bool:
    """Whether a company's location list suggests US-based or US-remote roles (pure).

    Targeting is US SWE roles with H-1B sponsorship, so a board that only ever
    posts in one non-US country is noise (this was real feedback after the
    first backfill: several global boards dominated the notifications).
    """
    low = (location or "").lower()
    if not low:
        return True  # unknown - let the poller's own filters decide
    if "remote" in low:
        return True
    us_hints = (
        "united states", ", usa", " usa", "san francisco", "new york", "seattle",
        "austin", "boston", "chicago", "los angeles", "denver", "atlanta",
        "palo alto", "mountain view", "sunnyvale", "san jose", "santa clara",
        "bellevue", "portland", "miami", "washington", "california", "texas",
        "colorado", "york, ny", ", ca", ", ny", ", wa", ", tx", ", ma",
    )
    return any(h in low for h in us_hints)


def load_cache() -> dict[str, dict]:
    """Load the probe-result cache ({"ats:token": {...}}); missing file -> {}."""
    if CACHE_PATH.exists():
        try:
            raw = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        except ValueError:
            print("[discover] cache unreadable; starting fresh")
            return {}
        # Entries written before the probe recorded engineering density carry no
        # "swe" key; re-probe them rather than scoring them as zero.
        return {k: v for k, v in raw.items() if "us" in v or v.get("alive") is False}
    return {}


def save_cache(cache: dict[str, dict]) -> None:
    """Persist the probe-result cache so re-runs do not re-probe the world."""
    CACHE_PATH.write_text(json.dumps(cache, indent=0, sort_keys=True), encoding="utf-8")


def fetch_text(url: str, client: httpx.Client) -> str:
    """GET a URL and return its body as text; network failures return ""."""
    try:
        r = client.get(url, timeout=60, follow_redirects=True)
        r.raise_for_status()
        return r.text
    except httpx.HTTPError as e:
        print(f"[discover] fetch failed {url}: {type(e).__name__}")
        return ""


def candidates_from_repos(client: httpx.Client) -> dict[tuple[str, str], str]:
    """Extract real board tokens from public new-grad/internship listing repos."""
    out: dict[tuple[str, str], str] = {}
    for url in LISTING_SOURCES:
        text = fetch_text(url, client)
        if not text:
            continue
        pairs = extract_tokens(text)
        for pair in pairs:
            out.setdefault(pair, f"listing repo: {url.split('/')[4]}")
        print(f"[discover] {len(pairs):5d} tokens from {url.split('/')[4]}")
    return out


def candidates_from_yc(client: httpx.Client, min_team: int) -> dict[tuple[str, str], str]:
    """Guess board tokens for YC companies that are currently hiring.

    YC startups sponsor H-1B/OPT far more readily than their size suggests,
    which is why they are worth the guess-and-verify cost. Sub-``min_team``
    companies are skipped: they very rarely run an ATS board or sponsor.
    """
    text = fetch_text(YC_HIRING_API, client)
    if not text:
        return {}
    companies = json.loads(text)
    out: dict[tuple[str, str], str] = {}
    kept = 0
    for c in companies:
        if (c.get("team_size") or 0) < min_team or c.get("nonprofit"):
            continue
        if not us_relevant(c.get("all_locations", "")):
            continue
        name = c.get("name", "")
        if is_excluded(name, c.get("slug", "")):
            continue
        kept += 1
        label = f"YC {c.get('batch', '?')} ({c.get('team_size')} ppl)"
        for token in slug_candidates(name, c.get("slug", "")):
            # Ashby and Greenhouse dominate at this stage; Lever is thinner but
            # cheap enough to include. SmartRecruiters is enterprise-only - skip.
            for ats in ("ashby", "greenhouse", "lever"):
                out.setdefault((ats, token), label)
    print(f"[discover] {kept} YC companies hiring (team >= {min_team}, US-relevant)")
    return out


def candidates_from_curated() -> dict[tuple[str, str], str]:
    """Token guesses for hand-listed majors and strong mid-size employers."""
    out: dict[tuple[str, str], str] = {}
    for word in CURATED.split():
        for token in slug_candidates(word):
            for ats in ("greenhouse", "ashby", "lever"):
                out.setdefault((ats, token), "curated")
    return out


CACHE_LOCK = threading.Lock()


def probe_stream(
    ats: str,
    tokens: list[str],
    cache: dict[str, dict],
    delay: float,
    deadline: float | None = None,
) -> dict[str, dict]:
    """Probe one provider's candidate tokens sequentially; returns live ones.

    One stream per provider, one request at a time, with a delay between
    requests - discovery is a burst of unknown tokens, so it stays gentler
    than the steady-state poll cycle.

    The cache is flushed to disk every CACHE_FLUSH_EVERY probes and the stream
    stops at `deadline`, so a long discovery run can be interrupted and resumed
    without re-probing anything it already answered.
    """
    live: dict[str, dict] = {}
    with polite_client() as client:
        for i, token in enumerate(tokens, 1):
            if deadline and time.monotonic() > deadline:
                print(f"[discover:{ats}] time budget reached at {i}/{len(tokens)}; "
                      "re-run to resume (answers are cached)")
                break
            key = f"{ats}:{token}"
            hit = cache.get(key)
            if hit is None:
                p = probe(ats, token, client)
                if p.alive is None:  # inconclusive: do not cache a maybe
                    time.sleep(delay * 4)
                    continue
                hit = {"alive": p.alive, "count": p.count, "swe": p.swe, "us": p.us}
                with CACHE_LOCK:
                    cache[key] = hit
                    if len(cache) % CACHE_FLUSH_EVERY == 0:
                        save_cache(cache)
                time.sleep(delay)
            if hit["alive"]:
                live[token] = hit
            if i % 250 == 0:
                print(f"[discover:{ats}] probed {i}/{len(tokens)}, {len(live)} live",
                      flush=True)
    print(f"[discover:{ats}] done: {len(live)} live of {len(tokens)} candidates", flush=True)
    return live


def write_watchlist(path: Path, data: dict[str, list[str]], header: str) -> None:
    """Write a watchlist YAML file with its explanatory header."""
    body = yaml.safe_dump(
        {ats: sorted(data.get(ats, [])) for ats in ATS_ORDER if data.get(ats)},
        sort_keys=True,
        default_flow_style=False,
    )
    path.write_text(header + body, encoding="utf-8")
    print(f"[discover] wrote {path} ({sum(len(v) for v in data.values())} tokens)")


TAIL_HEADER = """# watchlist_tail.yaml - the long tail of boards, polled hourly.
#
# Generated and verified by scripts/discover_boards.py (every token here
# resolved live against its ATS API when written). Sources: YC companies
# currently hiring, public new-grad/internship listing repos, curated
# mid-size employers.
#
# Why a second file: on GitHub runner IPs the ATS APIs tarpit us at ~7-30s per
# board, and the polite-client rule caps us at two sequential streams per
# provider. Splitting the corpus by cadence - majors in watchlist.yaml every
# 30 min, this file hourly - covers far more boards without ever opening a
# third stream against anyone. See .github/workflows/poll-tail.yml.
#
# Curation policy matches watchlist.yaml: no defense/ITAR/clearance shops
# (they cannot sponsor F-1 -> H-1B), no staffing or consulting mills,
# no boards that only ever post outside the US.
"""


# Relevance of a candidate's origin. Discovery finds far more live boards than
# a polite poll cycle can cover, so the corpus has to be *chosen*: a board that
# posts US new-grad SWE roles is worth a dozen boards that merely exist.
SOURCE_SCORES = {
    "New-Grad-Positions": 6,       # exactly the target population
    "curated": 6,                  # majors with known sponsorship history
    "2026-SWE-College-Jobs": 5,
    "2026-AI-College-Jobs": 5,
    "YC": 4,                       # YC startups sponsor well above their size
    "Summer2026-Internships": 2,   # internships only; lowest priority for a 2027 grad
}

# Board sizes that make a board more or less worth a slot in the cycle.
BIG_BOARD = 600   # global/enterprise boards: pagination cost, mostly non-US reqs
BUSY_BOARD = 400


def board_score(label: str, count: int, swe: int = 0, us: int = 0) -> int:
    """Rank one discovered board by origin and size (pure).

    Higher is better. Zero-posting boards score badly because the tail cadence
    cannot afford a slot for a board with nothing on it today; huge boards are
    penalized because they are almost always global enterprise career sites
    whose US new-grad density is near zero (this was real feedback after the
    first backfill).
    """
    score = 0
    for key, value in SOURCE_SCORES.items():
        if label.startswith(key) or key in label:
            score = value
            break
    if count == 0:
        score -= 4
    elif count > BIG_BOARD:
        score -= 3
    elif count <= BUSY_BOARD:
        score += 1
    # Engineering density decides more than size does: a 200-posting board with
    # no engineering roles is a retail or logistics career site.
    if count and swe == 0:
        score -= 8
    elif swe >= 3:
        score += 2
    # US presence decides eligibility, not just relevance: a board that posts
    # only outside the US cannot produce an H-1B-sponsored role here.
    if count and us == 0:
        score -= 8
    elif us >= 3:
        score += 2
    return score


# Discovery finds ~2,000 live boards for a corpus that can hold a few hundred,
# and a single global ranking would spend the whole budget on new-grad listing
# repos before reaching the first YC startup. Quotas keep each kind of source
# represented; unused quota is backfilled from whatever ranks next overall.
BUCKET_QUOTAS = {"primary": 0.50, "yc": 0.35, "other": 0.15}


def bucket_of(label: str) -> str:
    """Which quota bucket a candidate's origin belongs to (pure)."""
    if label.startswith("YC"):
        return "yc"
    if "New-Grad-Positions" in label or label == "curated" or "College-Jobs" in label:
        return "primary"
    return "other"


def rank_boards(
    live: dict[str, dict[str, dict]],
    labels: dict[tuple[str, str], str],
    budget: int,
) -> dict[str, list[str]]:
    """Pick the best `budget` boards across providers, by quota (pure, deterministic).

    Ties break on posting count then name so a re-run produces the same
    watchlist — a churning corpus would re-notify jobs it already sent.
    """
    scored = [
        (board_score(labels.get((ats, token), ""), info["count"],
                     info.get("swe", 0), info.get("us", 0)),
         info["count"], ats, token, bucket_of(labels.get((ats, token), "")))
        for ats, boards in live.items()
        for token, info in boards.items()
    ]
    scored.sort(key=lambda r: (-r[0], -r[1], r[2], r[3]))

    chosen: list[tuple[str, str]] = []
    taken: set[tuple[str, str]] = set()
    for bucket, share in BUCKET_QUOTAS.items():
        quota = round(budget * share)
        for _s, _c, ats, token, b in scored:
            if len(chosen) >= budget or quota <= 0:
                break
            if b == bucket and (ats, token) not in taken:
                chosen.append((ats, token))
                taken.add((ats, token))
                quota -= 1
    for _s, _c, ats, token, _b in scored:  # backfill unfilled quota
        if len(chosen) >= budget:
            break
        if (ats, token) not in taken:
            chosen.append((ats, token))
            taken.add((ats, token))

    picked: dict[str, list[str]] = {}
    for ats, token in chosen:
        picked.setdefault(ats, []).append(token)
    return {ats: sorted(toks) for ats, toks in picked.items()}


def main(argv: list[str] | None = None) -> int:
    """Discover, verify, and write new watchlist tokens; returns an exit code."""
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sources", default="repos,yc,curated")
    ap.add_argument("--min-team", type=int, default=10, help="YC minimum team size")
    ap.add_argument("--delay", type=float, default=0.25, help="seconds between probes")
    ap.add_argument("--dry-run", action="store_true", help="report only, write nothing")
    ap.add_argument("--limit", type=int, default=0, help="cap candidates per ATS (testing)")
    ap.add_argument("--include-sr", action="store_true",
                    help="also discover SmartRecruiters boards (~80%% staffing firms)")
    ap.add_argument("--tail-budget", type=int, default=300,
                    help="how many discovered boards to keep (best-ranked first)")
    ap.add_argument("--max-seconds", type=float, default=0,
                    help="stop probing after N seconds (resume by re-running)")
    args = ap.parse_args(argv)

    sources = {s.strip() for s in args.sources.split(",") if s.strip()}
    candidates: dict[tuple[str, str], str] = {}
    with polite_client() as client:
        if "repos" in sources:
            candidates |= candidates_from_repos(client)
        if "yc" in sources:
            for pair, label in candidates_from_yc(client, args.min_team).items():
                candidates.setdefault(pair, label)
        if "curated" in sources:
            for pair, label in candidates_from_curated().items():
                candidates.setdefault(pair, label)

    # Only the CORE watchlist is "already known". The tail file is generated
    # output: treating it as known would let each run inherit the last run's
    # picks and quietly drop everything it did not re-probe.
    if not args.include_sr:
        # SmartRecruiters extraction is dominated by staffing and consulting
        # firms (session 2b), and its limit=1 probe cannot measure engineering
        # density, so discovered SR boards are opt-in. The dozen curated SR
        # boards in the core watchlist are unaffected.
        dropped = [k for k in candidates if k[0] == "smartrecruiters"]
        for k in dropped:
            del candidates[k]
        print(f"[discover] skipped {len(dropped)} SmartRecruiters candidates "
              "(--include-sr to keep them)")

    known = load_watchlist("core")
    known_pairs = {(ats, t.lower()) for ats, toks in known.items() for t in toks}
    todo: dict[str, list[str]] = {ats: [] for ats in ATS_ORDER}
    for (ats, token), label in candidates.items():
        if (ats, token.lower()) in known_pairs or is_excluded(label, token):
            continue
        todo[ats].append(token)
    if args.limit:
        todo = {ats: toks[: args.limit] for ats, toks in todo.items()}
    print(f"[discover] {sum(len(v) for v in todo.values())} new candidates to probe: "
          + ", ".join(f"{ats}={len(t)}" for ats, t in todo.items()))

    cache = load_cache()
    deadline = time.monotonic() + args.max_seconds if args.max_seconds else None
    with ThreadPoolExecutor(max_workers=len(ATS_ORDER)) as pool:
        futures = {
            ats: pool.submit(probe_stream, ats, toks, cache, args.delay, deadline)
            for ats, toks in todo.items() if toks
        }
        live = {ats: f.result() for ats, f in futures.items()}
    save_cache(cache)

    verified = sum(len(v) for v in live.values())
    found = rank_boards(live, candidates, args.tail_budget)
    total = sum(len(v) for v in found.values())
    print(f"\n[discover] {verified} live boards verified; kept the top {total} "
          f"(--tail-budget {args.tail_budget})")
    for ats, toks in sorted(found.items()):
        print(f"  {ats}: {len(toks)}")
    if args.dry_run:
        print(json.dumps(found, indent=1))
        return 0

    # The tail file is generated, not hand-edited: it is rewritten from the
    # current ranking so the corpus stays at its budget instead of growing
    # every time discovery runs. Probe answers are cached, so a full re-run
    # over all sources is cheap.
    write_watchlist(TAIL_WATCHLIST_PATH, found, TAIL_HEADER)
    print(f"[discover] core watchlist untouched at {WATCHLIST_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

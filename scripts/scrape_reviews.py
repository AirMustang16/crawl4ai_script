import os
import json
import asyncio
from typing import Any, Dict, List, Optional
from datetime import datetime
import re
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse

from pydantic import BaseModel, Field
from dotenv import load_dotenv

# Load .env early
load_dotenv()

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CrawlerRunConfig,
    CacheMode,
    JsonCssExtractionStrategy,
    LLMExtractionStrategy,
    LLMConfig,
)
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.content_filter_strategy import PruningContentFilter

RAW_DIR = "/Users/omar/projects/crawl4ai_script/data/raw"
DEBUG_DIR = "/Users/omar/projects/crawl4ai_script/data/debug"
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(DEBUG_DIR, exist_ok=True)

# G2 consent banner accept
G2_CONSENT_JS = """
(async () => {
  const delay = (ms) => new Promise(r => setTimeout(r, ms));
  const btn = document.querySelector('#onetrust-accept-btn-handler');
  if (btn) {
    try { btn.click(); } catch(e) {}
    await delay(300);
  }
  return !!btn;
})();
"""

# Helper: read unified pages_to_scrape (CRAWL_MAX_CLICKS is deprecated but supported as fallback)
def get_pages_to_scrape() -> int:
    p = os.getenv("PAGES_TO_SCRAPE")
    if p and p.isdigit():
        return max(1, int(p))
    # Back-compat: CRAWL_MAX_CLICKS was number of in-page clicks; approximate pages as clicks+1
    c = os.getenv("CRAWL_MAX_CLICKS")
    if c and c.isdigit():
        return max(1, int(c) + 1)
    return 1

CSS_SCHEMAS: Dict[str, Dict[str, Any]] = {
    "www.softwareadvice.com": {
        "name": "SoftwareAdvice Reviews",
        "baseSelector": "#reviews-list [data-testid='textReview']",
        "fields": [
            {"name": "author", "selector": "[data-testid='reviewer-first-name']", "type": "text"},
            {"name": "date", "selector": "[data-testid='reviewed-date']", "type": "text"},
            {"name": "title", "selector": "p.text-2xl.font-bold", "type": "text"},
            {"name": "rating_raw", "selector": "[data-testid='review-overall-rating-value']", "type": "text"},
            {"name": "body", "selector": "div.relative > div.flex.w-full.flex-col.gap-y-4 > p.text-sm.text-grey-91", "type": "text"},
            {"name": "pros", "selector": "div.relative > div.flex.w-full.flex-col.gap-y-4 > div:nth-child(3) > p.text-sm.text-grey-91", "type": "text"},
            {"name": "cons", "selector": "div.relative > div.flex.w-full.flex-col.gap-y-4 > div:nth-child(4) > p.text-sm.text-grey-91", "type": "text"},
            {"name": "rating_breakdown_raw", "selector": "div.relative > div.flex.w-full.flex-col.gap-y-4 > div.flex.flex-col", "type": "text"},
        ],
    },
    "www.g2.com": {
        "name": "G2 Reviews (Jira)",
        "baseSelector": "#reviews > div > div.nested-ajax-loading > div > div",
        "fields": [
            # Reviewer name and role
            {"name": "author", "selector": "article div.elv-flex.elv-justify-between div.elv-flex.elv-items-center div.elv-flex.elv-items-center > div", "type": "text"},
            {"name": "author_role", "selector": "article div.elv-flex.elv-justify-between div.elv-flex.elv-items-center > div > div:nth-child(2)", "type": "text"},
            # Date
            {"name": "date", "selector": "article div.elv-flex.elv-justify-between > div:nth-child(2) > span", "type": "text"},
            # Review details container anchor
            {"name": "title", "selector": "article > div.elv-flex.elv-flex-col.elv-gap-y-4.elv-py-3.md\\:elv-py-4 > div:nth-child(1) > div", "type": "text"},
            {"name": "rating_raw", "selector": "article > div.elv-flex.elv-flex-col.elv-gap-y-4.elv-py-3.md\\:elv-py-4 > div.elv-flex.elv-flex-col.elv-gap-y-3 > div:nth-child(1)", "type": "text"},
            {"name": "pros", "selector": "article > div.elv-flex.elv-flex-col.elv-gap-y-4.elv-py-3.md\\:elv-py-4 > div.elv-flex.elv-flex-col.elv-gap-y-3 > section:nth-child(2)", "type": "text"},
            {"name": "cons", "selector": "article > div.elv-flex.elv-flex-col.elv-gap-y-4.elv-py-3.md\\:elv-py-4 > div.elv-flex.elv-flex-col.elv-gap-y-3 > section:nth-child(3)", "type": "text"},
            {"name": "body", "selector": "article > div.elv-flex.elv-flex-col.elv-gap-y-4.elv-py-3.md\\:elv-py-4 > div.elv-flex.elv-flex-col.elv-gap-y-3 > div.js-log-click", "type": "text"},
        ],
    },
}

READY_SELECTORS: Dict[str, str] = {
    "www.softwareadvice.com": "#reviews-list [data-testid='textReview']",
    "www.g2.com": "#reviews > div > div.nested-ajax-loading > div > div",
}

SA_CLOSE_JS = """
(async () => {
  const delay = (ms) => new Promise(r => setTimeout(r, ms));
  const sel = "#__faas-form-app > div > div.sb.bkg-light.card.padding-medium > i";
  let btn = document.querySelector(sel)
    || document.querySelector("[data-modal-role='close-button']")
    || document.querySelector("i.modal-close[aria-label='x']");
  if (btn) {
    btn.scrollIntoView({behavior: "instant", block: "center"});
    try { btn.click(); } catch(e) {}
    await delay(500);
  }
  return !!btn;
})();
"""

SA_EXPAND_READ_MORE_JS = """
(async () => {
  const delay = (ms) => new Promise(r => setTimeout(r, ms));
  let clicks = 0;
  document.querySelectorAll('#reviews-list [data-testid=\"textReview\"] p').forEach(el => {
    const txt = (el.textContent || '').trim().toLowerCase();
    if (/(read more|show more)/i.test(txt)) {
      const clickable = el.closest('div.cursor-pointer') || el.parentElement || el;
      try { clickable.click(); clicks++; } catch(e) {}
    }
  });
  if (clicks > 0) await delay(800);
  return clicks;
})();
"""

# SoftwareAdvice: click Next button in pager (target container and Next-labeled button), wait for page counter or first card title to change
SA_NEXT_PAGE_JS = """
(async () => {
  const delay = (ms) => new Promise(r => setTimeout(r, ms));
  const firstTitleSel = '#reviews-list [data-testid=\\"textReview\\"] p.text-2xl.font-bold';
  const contSel = '#generatedResults > span > section.gap\\:3.flex.justify-between.sm\\:justify-start.sm\\:gap-2\\.5';

  function readCounter() {
    const txt = document.body.innerText || '';
    const m = txt.match(/Showing\s+(\d+)\s*[-–]\s*(\d+)\s+of\s+(\d+)/i);
    if (m) {
      return { start: parseInt(m[1], 10), end: parseInt(m[2], 10), total: parseInt(m[3], 10) };
    }
    return null;
  }

  const firstBefore = document.querySelector(firstTitleSel)?.textContent?.trim() || '';
  const counterBefore = readCounter();

  // Scroll to bottom to ensure pager is visible
  window.scrollTo(0, document.body.scrollHeight);
  await delay(400);

  const cont = document.querySelector(contSel);
  const buttons = cont ? Array.from(cont.querySelectorAll('button')) : [];
  let btn = buttons.find(el => /next|›|→/i.test((el.textContent||'').trim())) || buttons[buttons.length - 1] || null;
  if (!btn) return false;

  btn.scrollIntoView({behavior: 'instant', block: 'center'});
  try {
    btn.dispatchEvent(new MouseEvent('mousedown', {bubbles: true}));
    btn.dispatchEvent(new MouseEvent('mouseup', {bubbles: true}));
    btn.click();
  } catch(e) {}

  // Wait for counter to advance or first title to change (max ~10s)
  let advanced = false;
  for (let i = 0; i < 20; i++) {
    await delay(500);
    const c = readCounter();
    if (counterBefore && c && (c.start > counterBefore.start)) { advanced = true; break; }
    const nowTitle = document.querySelector(firstTitleSel)?.textContent?.trim() || '';
    if (nowTitle && nowTitle !== firstBefore) { advanced = true; break; }
  }

  if (advanced) {
    window.scrollTo(0, 0);
    await delay(300);
  }
  return advanced;
})();
"""

# Navigation log injector: clicks Next (excluding .back), logs before/after into #c4ai-nav-log
SA_NAV_LOG_JS = """
(async () => {
  const delay = (ms) => new Promise(r => setTimeout(r, ms));
  const firstTitleSel = '#reviews-list [data-testid=\\"textReview\\"] p.text-2xl.font-bold';
  const contSel = '#generatedResults > span > section.gap\\:3.flex.justify-between.sm\\:justify-start.sm\\:gap-2\\.5';

  function readCounterText() {
    const el = Array.from(document.querySelectorAll('body *'))
      .find(n => /Showing\s+\d+\s*[-–]\s*\d+\s+of\s+\d+/i.test(n.textContent || ''));
    return el ? el.textContent.trim() : '';
  }

  const before = {
    firstTitle: document.querySelector(firstTitleSel)?.textContent?.trim() || '',
    counterText: readCounterText()
  };

  // Ensure pager visible
  window.scrollTo(0, document.body.scrollHeight);
  await delay(300);

  const cont = document.querySelector(contSel);
  const buttons = cont ? Array.from(cont.querySelectorAll('button')) : [];
  // Exclude buttons with class 'back'
  const candidates = buttons.filter(el => !el.classList.contains('back'));
  let btn = candidates.find(el => /next|›|→/i.test((el.textContent||'').trim())) || candidates[candidates.length - 1] || null;

  const logEl = document.getElementById('c4ai-nav-log') || (function(){ const s = document.createElement('span'); s.id = 'c4ai-nav-log'; s.style.display='none'; document.body.appendChild(s); return s; })();

  if (!btn) {
    logEl.textContent = JSON.stringify({ action: 'no-button', before });
    return false;
  }

  const btnInfo = { text: (btn.textContent||'').trim(), classes: btn.className };
  btn.scrollIntoView({behavior: 'instant', block: 'center'});
  try { btn.click(); } catch(e) {}

  // Wait for either counter or firstTitle to change
  let changed = false;
  for (let i = 0; i < 20; i++) {
    await delay(500);
    const nowTitle = document.querySelector(firstTitleSel)?.textContent?.trim() || '';
    const nowCounter = readCounterText();
    if ((before.firstTitle && nowTitle && nowTitle !== before.firstTitle) || (before.counterText && nowCounter && nowCounter !== before.counterText)) {
      changed = true; break;
    }
  }

  const after = {
    firstTitle: document.querySelector(firstTitleSel)?.textContent?.trim() || '',
    counterText: readCounterText(),
    changed
  };
  logEl.textContent = JSON.stringify({ action: 'clicked-next', btn: btnInfo, before, after });
  if (changed) { window.scrollTo(0, 0); await delay(200); }
  return changed;
})();
"""

# Schema to extract the nav log and quick state after a nav click
SA_NAV_LOG_SCHEMA = {
    "name": "Nav Log",
    "baseSelector": "body",
    "fields": [
        {"name": "nav_log", "selector": "#c4ai-nav-log", "type": "text"},
        {"name": "first_title", "selector": "#reviews-list [data-testid='textReview'] p.text-2xl.font-bold", "type": "text"},
        {"name": "showing_text", "selector": "body", "type": "text"}
    ]
}

class ReviewItem(BaseModel):
    title: Optional[str] = Field(None)
    body: Optional[str] = Field(None)
    author: Optional[str] = Field(None)
    date: Optional[str] = Field(None)
    rating: Optional[float] = Field(None)
    pros: Optional[str] = None
    cons: Optional[str] = None

LLM_ARRAY_SCHEMA = {
    "type": "array",
    "items": ReviewItem.model_json_schema(),
}

CLICK_MORE_JS = """
(async () => {
  const delay = (ms) => new Promise(r => setTimeout(r, ms));
  const candidates = [...document.querySelectorAll("button, a")];
  let btn = candidates.find(el => /next|load more|show more|more reviews|more/i.test(el.textContent || ""));
  if (!btn) return false;
  btn.scrollIntoView({behavior: "instant", block: "center"});
  btn.click();
  await delay(1200);
  window.scrollTo(0, document.body.scrollHeight);
  await delay(1200);
  return true;
})();
"""


def domain_from_url(url: str) -> str:
    return urlparse(url).netloc

_def_counter: Dict[str, int] = {}

def _debug_path(dom: str, label: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    _def_counter[dom] = _def_counter.get(dom, 0) + 1
    seq = _def_counter[dom]
    dom_dir = os.path.join(DEBUG_DIR, dom.replace(".", "_"))
    os.makedirs(dom_dir, exist_ok=True)
    return os.path.join(dom_dir, f"{ts}_{seq:03d}_{label}.txt")


def parse_rating_breakdown(text: Optional[str]) -> Optional[List[Dict[str, Any]]]:
    if not text:
        return None
    s = text.strip()
    s = re.sub(r"(?i)\bratings\s*breakdown\b", "", s).strip()
    if not s:
        return None
    items: List[Dict[str, Any]] = []
    for val_str, title in re.findall(r"([0-9](?:\.[0-9])?)\s*([^0-9]+?)(?=[0-9]|$)", s):
        try:
            val = float(val_str)
        except Exception:
            continue
        title_clean = title.strip().strip(':').strip()
        if not title_clean:
            continue
        if 0.0 <= val <= 5.0:
            items.append({"title": title_clean, "value": val})
    return items or None


def merge_items(base: List[Dict[str, Any]], new: List[Dict[str, Any]]):
    seen = set()
    out: List[Dict[str, Any]] = []
    def key(it: Dict[str, Any]):
        return (
            (it.get('author') or '').strip().lower(),
            (it.get('date') or '').strip().lower(),
            (it.get('title') or '').strip().lower(),
            (it.get('body') or '')[:120].strip().lower(),
        )
    for it in base:
        k = key(it)
        if k in seen:
            continue
        seen.add(k)
        out.append(it)
    added = 0
    for it in new:
        k = key(it)
        if k in seen:
            continue
        seen.add(k)
        out.append(it)
        added += 1
    return out, added

def build_g2_page_url(base_url: str, page_num: int) -> str:
    u = urlparse(base_url)
    q = parse_qs(u.query)
    if page_num > 1:
        q['page'] = [str(page_num)]
    else:
        # remove page param for first page
        if 'page' in q:
            del q['page']
    new_q = urlencode({k: v[0] if isinstance(v, list) and len(v) == 1 else v for k, v in q.items()}, doseq=True)
    fragment = 'reviews'
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, fragment))

async def prep_site(crawler, url: str, dom: str, session_id: str) -> None:
    js_list = []
    if dom == "www.softwareadvice.com":
        js_list.append(SA_CLOSE_JS)
    elif dom == "www.g2.com":
        js_list.append(G2_CONSENT_JS)
    if not js_list:
        return
    run_conf = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        session_id=session_id,
        page_timeout=int(os.getenv("PAGE_TIMEOUT_MS", "90000")),
        wait_for="body",
        wait_for_timeout=int(os.getenv("WAIT_FOR_TIMEOUT_MS", "45000")),
        js_code=js_list,
    )
    print(f"[STEP] Prep site JS -> {dom}")
    await crawler.arun(url=url, config=run_conf)

async def extract_with_css(crawler, url: str, schema: Dict[str, Any], session_id: str, dom: str) -> List[Dict[str, Any]]:
    all_items: List[Dict[str, Any]] = []

    await prep_site(crawler, url, dom, session_id)

    run_conf = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        session_id=session_id,
        page_timeout=int(os.getenv("PAGE_TIMEOUT_MS", "90000")),
        wait_for=READY_SELECTORS.get(dom, "body"),
        wait_for_timeout=int(os.getenv("WAIT_FOR_TIMEOUT_MS", "45000")),
        extraction_strategy=JsonCssExtractionStrategy(schema),
    )

    print(f"[STEP] Load page (CSS) -> {url}")
    res = await crawler.arun(url=url, config=run_conf)
    try:
        md = getattr(res, "markdown", None)
        raw_md = getattr(md, "raw_markdown", None)
        if raw_md:
            p = _debug_path(dom, "initial_markdown")
            with open(p, "w", encoding="utf-8") as f:
                f.write(raw_md)
            print(f"[DEBUG] Saved markdown snapshot: {p}")
    except Exception:
        pass

    if res.success and res.extracted_content:
        try:
            all_items.extend(json.loads(res.extracted_content))
        except Exception:
            pass
    print(f"[STEP] Extracted items on first load: {len(all_items)}")

    # Domain-specific post-process on initial page
    if dom == "www.softwareadvice.com":
        more_conf_expand = run_conf.clone(js_only=True, js_code=[SA_EXPAND_READ_MORE_JS])
        print("[STEP] Expand 'Read More' on visible cards")
        res_expand = await crawler.arun(url=url, config=more_conf_expand)
        if res_expand.success and res_expand.extracted_content:
            try:
                expanded = json.loads(res_expand.extracted_content)
                if len(expanded) >= len(all_items):
                    all_items = expanded
                    print(f"[STEP] Items after expand: {len(all_items)}")
            except Exception:
                pass
        for it in all_items:
            rb_raw = it.get("rating_breakdown_raw")
            it["rating_breakdown"] = parse_rating_breakdown(rb_raw)
            if "rating_breakdown_raw" in it:
                del it["rating_breakdown_raw"]
    elif dom == "www.g2.com":
        def strip_g2_noise(s: Optional[str]) -> Optional[str]:
            if not s:
                return s
            txt = re.sub(r"Review collected by and hosted on G2\.com\.?", "", s).strip()
            txt = re.sub(r"\bShow More\b|\bRead More\b|\bRead Less\b", "", txt, flags=re.I).strip()
            return txt or None
        for it in all_items:
            for k in ("pros", "cons", "body", "title"):
                it[k] = strip_g2_noise(it.get(k))
            if it.get("title"):
                it["title"] = it["title"].split("\n")[0].strip()

    pages_to_scrape = get_pages_to_scrape()

    # Pagination strategy per domain
    if dom == "www.softwareadvice.com":
        agg, _ = merge_items([], all_items)
        for page_idx in range(1, pages_to_scrape):
            print(f"[STEP] Go to next page #{page_idx+1}")
            nav_conf = run_conf.clone(js_only=True, js_code=[SA_NAV_LOG_JS])
            res_nav = await crawler.arun(url=url, config=nav_conf)
            navlog_conf = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                session_id=session_id,
                page_timeout=int(os.getenv("PAGE_TIMEOUT_MS", "90000")),
                wait_for=READY_SELECTORS.get(dom, "body"),
                wait_for_timeout=int(os.getenv("WAIT_FOR_TIMEOUT_MS", "45000")),
                extraction_strategy=JsonCssExtractionStrategy(SA_NAV_LOG_SCHEMA),
            )
            res_log = await crawler.arun(url=url, config=navlog_conf)
            if res_log.success and res_log.extracted_content:
                try:
                    logs = json.loads(res_log.extracted_content)
                    if logs and isinstance(logs, list):
                        entry = logs[0]
                        raw_log = entry.get('nav_log') or ''
                        if raw_log:
                            print(f"[NAVLOG] {raw_log}")
                except Exception:
                    pass
            res_next = await crawler.arun(url=url, config=run_conf)
            got: List[Dict[str, Any]] = []
            if res_next.success and res_next.extracted_content:
                try:
                    got = json.loads(res_next.extracted_content)
                except Exception:
                    got = []
            res_expand2 = await crawler.arun(url=url, config=run_conf.clone(js_only=True, js_code=[SA_EXPAND_READ_MORE_JS]))
            if res_expand2.success and res_expand2.extracted_content:
                try:
                    got = json.loads(res_expand2.extracted_content) or got
                except Exception:
                    pass
            for it in got:
                rb_raw = it.get("rating_breakdown_raw")
                it["rating_breakdown"] = parse_rating_breakdown(rb_raw)
                if "rating_breakdown_raw" in it:
                    del it["rating_breakdown_raw"]
            agg, added = merge_items(agg, got)
            print(f"[STEP] Page {page_idx+1}: extracted={len(got)}, added={added}, total={len(agg)}")
            if added == 0:
                print("[STEP] No new items added; stopping pagination.")
                break
        all_items = agg
    elif dom == "www.g2.com":
        agg, _ = merge_items([], all_items)
        base_url = url
        for page_idx in range(2, pages_to_scrape + 1):
            page_url = build_g2_page_url(base_url, page_idx)
            print(f"[STEP] G2 navigate to page={page_idx} -> {page_url}")
            res_page = await crawler.arun(url=page_url, config=run_conf)
            got: List[Dict[str, Any]] = []
            if res_page.success and res_page.extracted_content:
                try:
                    got = json.loads(res_page.extracted_content)
                except Exception:
                    got = []
            def strip_g2_noise(s: Optional[str]) -> Optional[str]:
                if not s:
                    return s
                txt = re.sub(r"Review collected by and hosted on G2\.com\.?", "", s).strip()
                txt = re.sub(r"\bShow More\b|\bRead More\b|\bRead Less\b", "", txt, flags=re.I).strip()
                return txt or None
            for it in got:
                for k in ("pros", "cons", "body", "title"):
                    it[k] = strip_g2_noise(it.get(k))
                if it.get("title"):
                    it["title"] = it["title"].split("\n")[0].strip()
            agg, added = merge_items(agg, got)
            print(f"[STEP] G2 page {page_idx}: extracted={len(got)}, added={added}, total={len(agg)}")
            if added == 0:
                print("[STEP] No new G2 items added; stopping pagination.")
                break
        all_items = agg
    else:
        # Generic in-page click strategy approximated by pages_to_scrape-1 clicks
        clicks_to_try = max(0, pages_to_scrape - 1)
        for i in range(clicks_to_try):
            more_conf = run_conf.clone(js_only=True, js_code=[CLICK_MORE_JS])
            print(f"[STEP] Click 'Load more/Next' #{i+1}")
            res = await crawler.arun(url=url, config=more_conf)
            try:
                md = getattr(res, "markdown", None)
                raw_md = getattr(md, "raw_markdown", None)
                if raw_md:
                    p = _debug_path(dom, f"click_{i+1:02d}_markdown")
                    with open(p, "w", encoding="utf-8") as f:
                        f.write(raw_md)
                    print(f"[DEBUG] Saved markdown snapshot: {p}")
            except Exception:
                pass
            if not res.success:
                print("[WARN] Click resulted in unsuccessful response; stopping clicks.")
                break
            got: List[Dict[str, Any]] = []
            if res.extracted_content:
                try:
                    got = json.loads(res.extracted_content)
                except Exception:
                    got = []
            print(f"[STEP] Items after click {i+1}: {len(got)} (prev {len(all_items)})")
            if not got or (len(got) <= len(all_items)):
                print("[STEP] No additional reviews detected; stopping clicks.")
                break
            all_items = got

    return all_items

async def extract_with_llm(crawler, url: str, session_id: str, dom: str, provider: str = "ollama/llama3.3", api_token: Optional[str] = None) -> List[Dict[str, Any]]:
    md_gen = DefaultMarkdownGenerator(content_filter=PruningContentFilter(threshold=0.35, threshold_type="fixed"))
    llm_conf = LLMConfig(provider=provider, api_token=api_token)

    run_conf = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        session_id=session_id,
        page_timeout=int(os.getenv("PAGE_TIMEOUT_MS", "90000")),
        wait_for=READY_SELECTORS.get(dom, "body"),
        wait_for_timeout=int(os.getenv("WAIT_FOR_TIMEOUT_MS", "45000")),
        markdown_generator=md_gen,
        extraction_strategy=LLMExtractionStrategy(
            llm_config=llm_conf,
            schema=LLM_ARRAY_SCHEMA,
            extraction_type="schema",
            instruction=(
                "Extract ALL reviews present on the page into an array. "
                "For each item, include: title, body, author, date, rating (0-5 if present), pros, cons. "
                "Do not summarize; capture faithfully."
            ),
            extra_args={"temperature": 0, "top_p": 0.9, "max_tokens": 4000},
        ),
    )

    print(f"[STEP] Load page (LLM) -> {url}")
    res_items: List[Dict[str, Any]] = []
    res = await crawler.arun(url=url, config=run_conf)

    try:
        md = getattr(res, "markdown", None)
        raw_md = getattr(md, "raw_markdown", None)
        if raw_md:
            p = _debug_path(dom, "llm_initial_markdown")
            with open(p, "w", encoding="utf-8") as f:
                f.write(raw_md)
            print(f"[DEBUG] Saved markdown snapshot: {p}")
    except Exception:
        pass

    if res.success and res.extracted_content:
        try:
            res_items = json.loads(res.extracted_content)
        except Exception:
            res_items = []

    # Use pages_to_scrape for LLM too (approximate via re-extractions)
    pages_to_scrape = get_pages_to_scrape()
    for i in range(max(0, pages_to_scrape - 1)):
        more_conf = run_conf.clone(js_only=True, js_code=[CLICK_MORE_JS])
        print(f"[STEP] Click 'Load more/Next' (LLM) #{i+1}")
        res = await crawler.arun(url=url, config=more_conf)
        try:
            md = getattr(res, "markdown", None)
            raw_md = getattr(md, "raw_markdown", None)
            if raw_md:
                p = _debug_path(dom, f"llm_click_{i+1:02d}_markdown")
                with open(p, "w", encoding="utf-8") as f:
                    f.write(raw_md)
                print(f"[DEBUG] Saved markdown snapshot: {p}")
        except Exception:
            pass
        got = []
        if res.success and res.extracted_content:
            try:
                got = json.loads(res.extracted_content)
            except Exception:
                got = []
        if not got or (len(got) <= len(res_items)):
            print("[STEP] No additional reviews detected (LLM); stopping clicks.")
            break
        res_items = got

    return res_items


def dump_files(prefix: str, rows: List[Dict[str, Any]]):
    jsonl = os.path.join(RAW_DIR, f"{prefix}.jsonl")
    csvp = os.path.join(RAW_DIR, f"{prefix}.csv")
    with open(jsonl, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    import csv
    keys = sorted({k for r in rows for k in r.keys()}) if rows else []
    with open(csvp, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"[OK] wrote {jsonl} and {csvp} | rows={len(rows)}")

async def crawl_one(url: str) -> List[Dict[str, Any]]:
    dom = domain_from_url(url)
    schema = CSS_SCHEMAS.get(dom)
    session_id = f"session::{dom}"

    headful = os.getenv("HEADFUL", "0") == "1"
    keep_open = os.getenv("KEEP_BROWSER_OPEN", "0") == "1"
    browser_conf = BrowserConfig(headless=not headful, java_script_enabled=True)

    items: List[Dict[str, Any]] = []

    crawler = AsyncWebCrawler(config=browser_conf)
    await crawler.__aenter__()
    try:
        if schema:
            items = await extract_with_css(crawler, url, schema, session_id, dom)

        enable_llm = os.getenv("ENABLE_LLM_FALLBACK", "0") == "1"
        if enable_llm and len(items) < 5:
            llm_items = await extract_with_llm(
                crawler,
                url,
                session_id,
                dom=dom,
                provider=os.getenv("LLM_PROVIDER", "ollama/llama3.3"),
                api_token=os.getenv("LLM_API_TOKEN") or None,
            )
            if len(llm_items) > len(items):
                items = llm_items

        for it in items:
            it.setdefault("source_url", url)
            it.setdefault("source_domain", dom)

        safe_dom = dom.replace(".", "_")
        dump_files(f"{safe_dom}_jira_reviews", items)
    finally:
        if not keep_open:
            await crawler.__aexit__(None, None, None)
        else:
            print("[INFO] KEEP_BROWSER_OPEN=1 set; leaving browser open.")
    return items

async def main():
    urls = [
        "https://www.softwareadvice.com/project-management/atlassian-jira-profile/reviews/",
        "https://www.g2.com/products/jira/reviews#reviews",
    ]
    print("[STEP] Starting crawls...")
    results_map = {}
    for u in urls:
        try:
            items = await crawl_one(u)
            results_map[u] = len(items)
        except Exception as e:
            print(f"[ERROR] Failed crawling {u}: {e}")
            results_map[u] = 0
    print(results_map)

if __name__ == "__main__":
    asyncio.run(main())

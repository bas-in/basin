/**
 * site/ gate — renders index.html and docs.html in a real browser and fails on
 * the defects that static inspection cannot see.
 *
 * Why a browser: every bug this catches was invisible in the source. An image
 * stretched to the wrong aspect ratio, body copy that lands under the
 * legibility floor at one width only, an element that pushes the page sideways
 * on a 320px phone, a <use href="#…"> whose symbol was renamed so the brand
 * mark silently stops painting, an anchor pointing at an id that no longer
 * exists, a font or script quietly fetched off-box — the HTML reads fine in
 * all of those cases.
 *
 * Each check states what it measured, not just pass/fail, so a green run is
 * evidence rather than an assertion. Run:
 *
 *   node scripts/check-render.mjs                 # serves site/ itself
 *   node scripts/check-render.mjs --selftest      # prove the checks can fail
 *
 * Basin has no package.json, so Playwright is resolved from whichever
 * node_modules on this machine has it — set PLAYWRIGHT_NODE_MODULES to point
 * at one explicitly.
 */

import { createServer } from 'http';
import { readFile, readdir } from 'fs/promises';
import { existsSync } from 'fs';
import { createRequire } from 'module';
import { resolve, dirname, extname, join, normalize } from 'path';
import { fileURLToPath, pathToFileURL } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPO = resolve(__dirname, '..');
const SITE = resolve(REPO, 'site');

// ---------------------------------------------------------------------------
// Playwright, from wherever it lives on this box
// ---------------------------------------------------------------------------
async function loadPlaywright() {
  const tried = [];
  try { return await import('playwright'); } catch { tried.push('<bare specifier>'); }

  // Walk up a couple of levels and look for a sibling checkout that has one.
  // Bounded on purpose: two levels covers "repo beside repo" and "repo inside
  // a fleet directory", and stops well short of scanning $HOME.
  const candidates = [];
  if (process.env.PLAYWRIGHT_NODE_MODULES) candidates.push(process.env.PLAYWRIGHT_NODE_MODULES);
  const seen = new Set();
  for (const up of ['..', '../..']) {
    const root = resolve(REPO, up);
    if (seen.has(root)) continue;
    seen.add(root);
    let entries = [];
    try { entries = await readdir(root); } catch { continue; }
    for (const e of entries) {
      if (e.startsWith('.')) continue;
      candidates.push(join(root, e, 'node_modules'));
      // Monorepo-shaped siblings keep their deps a level or two down.
      candidates.push(join(root, e, 'web', 'node_modules'));
      for (const app of ['desktop', 'web', 'site']) {
        candidates.push(join(root, e, 'apps', app, 'node_modules'));
      }
    }
  }

  for (const root of candidates) {
    if (!root || !existsSync(join(root, 'playwright'))) continue;
    try {
      const req = createRequire(join(root, '__resolve__.js'));
      const entry = req.resolve('playwright');
      const raw = await import(pathToFileURL(entry).href);
      // Playwright is CommonJS. Importing it by PATH skips Node's named-export
      // detection, so the launchers arrive under `default` rather than as
      // named exports — the shape differs from a bare `import('playwright')`
      // and cost an hour of "cannot read properties of undefined".
      const mod = raw?.chromium ? raw : raw?.default;
      // A resolvable `playwright` is not necessarily a usable one: a stub or a
      // partial install resolves fine and then dies three calls later.
      if (!mod?.chromium?.launch) { tried.push(`${root} (no chromium launcher)`); continue; }
      console.log(`check-render: using playwright from ${root}`);
      return mod;
    } catch (e) { tried.push(`${root} (${e.code || e.message})`); }
  }

  console.error('check-render: could not load playwright.\n  tried: ' + tried.join('\n         ') +
    '\n  set PLAYWRIGHT_NODE_MODULES=/path/to/node_modules');
  process.exit(2);
}

const VIEWPORTS = [
  { w: 1920, h: 1080, label: 'desktop-xl' },
  { w: 1440, h: 900, label: 'desktop' },
  { w: 1280, h: 800, label: 'laptop' },
  { w: 1024, h: 768, label: 'tablet-landscape' },
  { w: 768, h: 1024, label: 'tablet' },
  { w: 430, h: 932, label: 'phone-large' },
  { w: 390, h: 844, label: 'phone' },
  { w: 320, h: 720, label: 'phone-min' },
];

const PAGES = ['index.html', 'docs.html'];

const MIME = {
  '.html': 'text/html; charset=utf-8', '.css': 'text/css', '.js': 'text/javascript',
  '.mjs': 'text/javascript', '.json': 'application/json', '.md': 'text/markdown; charset=utf-8',
  '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.svg': 'image/svg+xml',
  '.woff2': 'font/woff2', '.woff': 'font/woff', '.txt': 'text/plain; charset=utf-8',
  '.ico': 'image/x-icon', '.webp': 'image/webp',
};

function serve(root) {
  return new Promise(ok => {
    const s = createServer(async (req, res) => {
      const rel = normalize(decodeURIComponent(req.url.split('?')[0])).replace(/^(\.\.[/\\])+/, '');
      let file = join(root, rel);
      if (!extname(file)) file = join(file, 'index.html');
      try {
        const body = await readFile(file);
        res.writeHead(200, { 'content-type': MIME[extname(file)] || 'application/octet-stream' });
        res.end(body);
      } catch {
        res.writeHead(404).end('not found');
      }
    });
    s.listen(0, '127.0.0.1', () => ok(s));
  });
}

// ---------------------------------------------------------------------------
// Findings
// ---------------------------------------------------------------------------
const findings = [];
const notes = [];
const fail = (check, where, detail) => findings.push({ check, where, detail });
const note = (s) => notes.push(s);

// ---------------------------------------------------------------------------
// Per-viewport, per-theme DOM checks
// ---------------------------------------------------------------------------
async function inspect(page, opts = {}) {
  return page.evaluate(async ({ isDark }) => {
    const out = {
      overflow: null, imgs: [], smallText: [], deadAnchors: [],
      hiddenPairs: [], themedShots: [], marks: [],
    };

    // 1 · page-level horizontal overflow.
    // scrollWidth alone is not enough: html{overflow-x:clip} makes an
    // overflowing child report the clipped width, so measure the children too.
    const de = document.documentElement;
    const bleed = [];
    document.querySelectorAll('body *').forEach(el => {
      const cs = getComputedStyle(el);
      if (cs.position === 'fixed') return;
      const r = el.getBoundingClientRect();
      if (r.width <= 0) return;
      // Deliberate designs that overflow their own scroll container are fine
      // (the benchmark table, the code blocks); only flag elements that push
      // the PAGE sideways.
      let p = el.parentElement, contained = false;
      while (p && p !== document.body) {
        const pcs = getComputedStyle(p);
        if (/auto|scroll|hidden|clip/.test(pcs.overflowX)) { contained = true; break; }
        p = p.parentElement;
      }
      if (contained) return;
      // Something parked wholly off-screen left adds no horizontal scroll —
      // only content crossing the RIGHT edge does, plus anything straddling
      // the left edge and therefore partly unreachable.
      if (r.right <= 0) return;
      if (r.right > window.innerWidth + 1 || r.left < -1) {
        bleed.push({
          tag: el.tagName, cls: String(el.className.baseVal ?? el.className).slice(0, 50),
          left: Math.round(r.left), right: Math.round(r.right),
        });
      }
    });
    out.overflow = { docW: de.scrollWidth, winW: window.innerWidth, bleed: bleed.slice(0, 6) };

    // 2 · every visible raster image at its true aspect ratio, and — on a 2×
    // context — actually resolving to a 2× source when one is offered.
    // naturalWidth is DENSITY-CORRECTED: a candidate picked via a "2x"
    // descriptor reports half its real pixel width, so comparing it against
    // the device pixels needed double-counts the ratio. Re-load currentSrc
    // bare to get its true pixel size.
    const trueSize = async (url) => await new Promise(res => {
      const probe = new Image();
      probe.onload = () => res([probe.naturalWidth, probe.naturalHeight]);
      probe.onerror = () => res([0, 0]);
      probe.src = url;
    });
    for (const im of document.querySelectorAll('img')) {
      const r = im.getBoundingClientRect();
      if (r.width < 4 || r.height < 4) continue;
      if (!im.naturalWidth || !im.naturalHeight) continue;
      const rendered = r.width / r.height;
      const natural = im.naturalWidth / im.naturalHeight;
      const url = im.currentSrc || im.src;
      const [px, py] = await trueSize(url);
      // Only `fill` (the default) stretches pixels. cover/contain/none crop or
      // letterbox instead, so a box ratio that differs from the source ratio
      // is the intended design, not a defect.
      const fit = getComputedStyle(im).objectFit;
      out.imgs.push({
        file: url.split('/').pop(),
        css: `${Math.round(r.width)}x${Math.round(r.height)}`,
        nat: `${im.naturalWidth}x${im.naturalHeight}`,
        realPx: `${px}x${py}`,
        skewPct: fit === 'fill' ? +(Math.abs(rendered / natural - 1) * 100).toFixed(1) : 0,
        objectFit: fit,
        offersSrcset: !!(im.srcset || im.closest('picture')?.querySelector('source[srcset]')),
        upscale: px ? +(r.width * devicePixelRatio / px).toFixed(2) : 0,
      });
    }

    // 3 · legibility floor for real body copy.
    const TEXTY = new Set(['P', 'LI', 'DD', 'DT', 'TD', 'TH', 'SPAN', 'B', 'EM', 'STRONG', 'A', 'CODE']);
    // `body *`, not `main *`. A landing with no <main> made this scan almost
    // nothing elsewhere in the suite, so the floor passed vacuously and a
    // planted 9px paragraph went straight through the self-test.
    document.querySelectorAll('body *').forEach(el => {
      if (!TEXTY.has(el.tagName)) return;
      const own = [...el.childNodes].some(n => n.nodeType === 3 && n.textContent.trim().length > 12);
      if (!own) return;
      const r = el.getBoundingClientRect();
      if (r.width < 4 || r.height < 4) return;
      const cs = getComputedStyle(el);
      // Eyebrows, field labels, elevation tags and axis ticks are set in
      // small-caps mono by design; the floor is for prose.
      if (cs.visibility === 'hidden' || cs.textTransform === 'uppercase') return;
      const size = parseFloat(cs.fontSize);
      if (size < 12) {
        out.smallText.push({ tag: el.tagName, size, text: el.textContent.trim().slice(0, 40) });
      }
    });

    // 4 · every same-page fragment link resolves. Both of Basin's pages are
    // static documents with real ids — neither is a hash router — so this
    // applies to both.
    document.querySelectorAll('a[href^="#"]').forEach(a => {
      const id = a.getAttribute('href').slice(1);
      if (!id) return;
      if (!document.getElementById(id) && !document.querySelector(`[name="${CSS.escape(id)}"]`)) {
        out.deadAnchors.push({ href: '#' + id, text: a.textContent.trim().slice(0, 30) });
      }
    });

    // 5 · the brand mark is drawn once as an SVG <symbol> and stamped with
    // <use href="#basin-mark"> in the header, the hero, the SDK rail and the
    // footer. Rename the symbol and every stamp silently renders nothing —
    // no console error, no layout shift, just a hole where the logo was.
    // Resolve each reference and confirm the instance actually has ink.
    document.querySelectorAll('use').forEach(u => {
      const href = u.getAttribute('href') || u.getAttribute('xlink:href') || '';
      if (!href.startsWith('#')) return;
      const target = document.getElementById(href.slice(1));
      const host = u.ownerSVGElement;
      const hostBox = host ? host.getBoundingClientRect() : { width: 0, height: 0 };
      // A <use> inside the hidden sprite has no box of its own; only judge the
      // stamps that are supposed to be visible.
      if (hostBox.width < 4 || hostBox.height < 4) return;
      let painted = 0;
      try { painted = target ? target.querySelectorAll('path,rect,circle,ellipse,line,polygon,polyline').length : 0; } catch { painted = 0; }
      out.marks.push({
        href, resolved: !!target, shapes: painted,
        box: `${Math.round(hostBox.width)}x${Math.round(hostBox.height)}`,
      });
    });

    // 6 · theme-aware imagery, both mechanisms, decided from the DOM
    // relationship rather than the filename.
    const paintedEl = e => {
      const r = e.getBoundingClientRect();
      return r.width > 4 && r.height > 4 && getComputedStyle(e).visibility !== 'hidden';
    };
    out.hiddenPairs.push({
      scope: 'document',
      light: [...document.querySelectorAll('.only-light')].filter(paintedEl).length,
      dark: [...document.querySelectorAll('.only-dark')].filter(paintedEl).length,
    });

    document.querySelectorAll('img[data-light][data-dark]').forEach(im => {
      if (!paintedEl(im)) return;
      const want = new URL(im.getAttribute(isDark ? 'data-dark' : 'data-light'), location.href).href;
      const got = im.currentSrc || im.src || '';
      if (got && got !== want) out.themedShots.push({ file: got.split('/').pop(), wanted: want.split('/').pop() });
    });

    document.querySelectorAll('picture > source[media*="prefers-color-scheme"]').forEach(src => {
      const im = src.parentElement.querySelector('img');
      if (!im || !paintedEl(im)) return;
      const forDark = /dark/i.test(src.getAttribute('media') || '');
      const wantRaw = (isDark === forDark) ? (src.getAttribute('srcset') || '').split(/\s|,/)[0]
        : im.getAttribute('src');
      if (!wantRaw) return;
      const want = new URL(wantRaw, location.href).href;
      const got = im.currentSrc || im.src || '';
      if (got && got !== want) out.themedShots.push({ file: got.split('/').pop(), wanted: want.split('/').pop() });
    });

    return out;
  }, { isDark: !!opts.isDark });
}

async function checkPage(browser, base, path, theme, vp) {
  const ctx = await browser.newContext({
    viewport: { width: vp.w, height: vp.h }, deviceScaleFactor: 2, colorScheme: theme,
  });
  const page = await ctx.newPage();
  const where = `${path} ${vp.label}(${vp.w}) ${theme}`;

  const httpErrors = [];
  page.on('response', r => { if (r.status() >= 400) httpErrors.push(`${r.status()} ${r.url()}`); });
  page.on('pageerror', e => fail('js-error', where, e.message));

  // Every subresource must come from the same origin as the page.
  //
  // The site claims to be self-contained and air-gappable. That was true and
  // unenforced: nothing would have noticed a <script src="https://cdn…"> or a
  // Google Fonts @import, and both are the kind of thing that arrives in a
  // hurry and stays.
  const offOrigin = new Set();
  page.on('request', r => {
    const url = r.url();
    if (r.resourceType() === 'document') return;
    if (/^(data|blob|about|javascript):/.test(url)) return;
    if (url.startsWith(base)) return;
    offOrigin.add(`${r.resourceType()} ${url}`);
  });

  await page.goto(`${base}/${path}`, { waitUntil: 'networkidle' });
  // Reveal-on-scroll gates most of the page; force it so nothing is measured
  // while still at opacity 0 and mid-transform. A fast programmatic scroll
  // does not reliably fire an IntersectionObserver.
  await page.evaluate(() =>
    document.querySelectorAll('.reveal, .rv, [data-reveal]').forEach(e => e.classList.add('is-visible', 'in', 'is-in')));
  await page.evaluate(async () => {
    const H = document.body.scrollHeight;
    for (let y = 0; y < H; y += 400) { window.scrollTo(0, y); await new Promise(r => setTimeout(r, 20)); }
    window.scrollTo(0, 0);
  });
  await page.waitForTimeout(400);

  const r = await inspect(page, { isDark: theme === 'dark' });

  if (r.overflow.docW > r.overflow.winW + 1) {
    fail('h-overflow', where, `document is ${r.overflow.docW}px wide in a ${r.overflow.winW}px viewport`);
  }
  if (r.overflow.bleed.length) {
    fail('h-overflow', where,
      'elements pushing past the viewport: ' + r.overflow.bleed
        .map(b => `${b.tag}.${b.cls} [${b.left}→${b.right}]`).join('; '));
  }
  r.imgs.forEach(i => {
    if (i.skewPct > 1.5) {
      fail('img-distorted', where,
        `${i.file} drawn ${i.css} from a ${i.nat} source — ${i.skewPct}% off its true aspect ratio`);
    }
    // Vector art has no resolution to be short of. Only raster sources can be
    // upscaled into softness.
    const isVector = /\.svgx?(\?|#|$)/i.test(i.file);
    if (!isVector && i.offersSrcset && i.upscale > 1.15) {
      fail('img-soft', where,
        `${i.file} drawn ${i.css} at dpr2 from a ${i.realPx} file — upscaled ${i.upscale}×`);
    }
  });
  r.smallText.forEach(t =>
    fail('text-too-small', where, `<${t.tag.toLowerCase()}> at ${t.size}px: “${t.text}”`));
  r.deadAnchors.forEach(a =>
    fail('dead-anchor', where, `${a.href} (“${a.text}”) matches no element on the page`));
  r.marks.forEach(m => {
    if (!m.resolved) {
      fail('mark-unresolved', where, `<use href="${m.href}"> in a ${m.box} box resolves to nothing`);
    } else if (m.shapes === 0) {
      fail('mark-unresolved', where, `<use href="${m.href}"> resolves to a symbol with no drawable shapes`);
    }
  });
  r.themedShots.forEach(s =>
    fail('screenshot-wrong-theme', where,
      `showing ${s.file} in ${theme} mode; the ${theme} variant is ${s.wanted}`));
  r.hiddenPairs.forEach(p => {
    const live = theme === 'dark' ? p.dark : p.light;
    const dead = theme === 'dark' ? p.light : p.dark;
    if (dead > 0) fail('both-themes-visible', where,
      `${p.scope}: ${dead} off-theme image(s) painted alongside ${live} on-theme`);
    if (live === 0 && dead === 0) return;   // page does not use the paired model
    if (live === 0) fail('no-image-visible', where, `${p.scope}: nothing painted for the ${theme} theme`);
  });
  httpErrors.forEach(u => fail('http-error', where, u));
  offOrigin.forEach(u => fail('off-origin', where,
    `${u} — the site must be self-contained; vendor it under site/assets/`));

  await ctx.close();
  return r;
}

// ---------------------------------------------------------------------------
// Cross-page anchors: index.html ↔ docs.html
// ---------------------------------------------------------------------------
async function checkCrossPageAnchors(browser, base) {
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const page = await ctx.newPage();

  const idsOf = async (p) => {
    await page.goto(`${base}/${p}`, { waitUntil: 'networkidle' });
    await page.waitForTimeout(300);
    return new Set(await page.evaluate(() => [...document.querySelectorAll('[id]')].map(e => e.id)));
  };
  const linksOf = async (p) => {
    await page.goto(`${base}/${p}`, { waitUntil: 'networkidle' });
    return page.evaluate(() => [...document.querySelectorAll('a[href*=".html#"]')]
      .map(a => ({ href: a.getAttribute('href'), text: a.textContent.trim().slice(0, 30) })));
  };

  const ids = {};
  for (const p of PAGES) ids[p] = await idsOf(p);

  for (const from of PAGES) {
    for (const l of await linksOf(from)) {
      const [file, frag] = l.href.replace(/^\.\//, '').split('#');
      if (!ids[file]) continue;                       // external or unknown target
      if (!ids[file].has(frag)) {
        fail('dead-anchor', `${from} → ${l.href}`,
          `“${l.text}” points at #${frag}, which ${file} does not define`);
      }
    }
  }
  for (const p of PAGES) note(`${p}: ${ids[p].size} addressable ids`);
  await ctx.close();
}

// ---------------------------------------------------------------------------
// Self-test: break each invariant on purpose and demand the check notices.
// A gate that has quietly stopped failing looks exactly like one that works.
//
// The mutations pick their targets from the page rather than naming selectors,
// so a case whose mechanism the page does not use reports "n/a" rather than
// passing silently — an inapplicable check must not read as a working one.
// ---------------------------------------------------------------------------
async function selftest(browser, base) {
  const cases = ['img-distorted', 'text-too-small', 'h-overflow', 'dead-anchor',
    'mark-unresolved', 'both-themes-visible', 'screenshot-wrong-theme', 'off-origin'];
  let allCaught = true;
  for (const name of cases) {
    const ctx = await browser.newContext({
      viewport: { width: 1440, height: 900 }, deviceScaleFactor: 2, colorScheme: 'dark',
    });
    const page = await ctx.newPage();

    // The off-origin check is a request listener rather than a DOM
    // measurement, so this case cannot go through inspect() like the others.
    const sawOffOrigin = new Set();
    if (name === 'off-origin') {
      page.on('request', r => {
        const url = r.url();
        if (r.resourceType() === 'document') return;
        if (/^(data|blob|about|javascript):/.test(url)) return;
        if (url.startsWith(base)) return;
        sawOffOrigin.add(url);
      });
    }

    await page.goto(`${base}/index.html`, { waitUntil: 'networkidle' });
    await page.evaluate(() => document.querySelectorAll('.reveal').forEach(e => e.classList.add('is-visible')));
    await page.waitForTimeout(400);

    const applicable = await page.evaluate((which) => {
      const box = e => e.getBoundingClientRect();
      if (which === 'img-distorted') {
        // Must be a DECODED image: the check skips anything with no intrinsic
        // size, so mutating an undecoded one produces a false MISSED. The
        // threshold is 8px, not 40px — this page's only raster images are the
        // 18px and 22px suite marks, and a 40px floor would have made the case
        // report n/a while claiming to guard every image on the page.
        const im = [...document.querySelectorAll('img')]
          .filter(e => box(e).width > 8 && box(e).height > 8 && e.naturalWidth > 0)
          .sort((a, b) => box(b).width - box(a).width)[0];
        if (!im) return false;
        // object-fit must be forced to `fill`: the check deliberately ignores
        // skew under cover/contain, so on a cropped image the mutation would
        // be undetectable BY DESIGN and the case would report a false MISSED.
        im.style.setProperty('object-fit', 'fill', 'important');
        im.style.setProperty('height', Math.round(box(im).width * 2) + 'px', 'important');
        im.style.setProperty('aspect-ratio', 'auto', 'important');
        return true;
      }
      if (which === 'text-too-small') {
        const p = [...document.querySelectorAll('main p, section p, body p')]
          .find(e => e.textContent.trim().length > 40 && box(e).width > 40);
        if (!p) return false;
        p.style.setProperty('font-size', '9px', 'important');
        p.style.setProperty('text-transform', 'none', 'important');
        return true;
      }
      if (which === 'h-overflow') {
        // Straight onto <body>: the scan treats anything inside an overflow
        // container as contained, and this page has several, so an element
        // planted inside one could never be seen.
        const d = document.createElement('div');
        d.style.cssText = 'width:3000px;height:20px;background:red';
        document.body.appendChild(d);
        return true;
      }
      if (which === 'dead-anchor') {
        const a = [...document.querySelectorAll('a[href^="#"]')]
          .find(e => document.getElementById(e.getAttribute('href').slice(1)));
        if (!a) return false;
        a.setAttribute('href', '#section-that-does-not-exist');
        return true;
      }
      if (which === 'mark-unresolved') {
        // Rename the symbol, exactly as a careless refactor would. Every
        // <use> stays in place and keeps its box; only the ink disappears.
        const sym = document.querySelector('symbol[id]');
        if (!sym || !document.querySelector('use')) return false;
        sym.id = sym.id + '-renamed';
        return true;
      }
      if (which === 'both-themes-visible') {
        if (!document.querySelector('.only-light') || !document.querySelector('.only-dark')) return false;
        document.querySelectorAll('.only-light,.only-dark')
          .forEach(e => e.style.setProperty('display', 'block', 'important'));
        return true;
      }
      if (which === 'screenshot-wrong-theme') {
        const swap = document.querySelectorAll('img[data-light][data-dark]');
        const pair = document.querySelector('.only-light') && document.querySelector('.only-dark');
        const pics = document.querySelectorAll('picture > source[media*="prefers-color-scheme"]');
        if (!swap.length && !pair && !pics.length) return false;
        pics.forEach(sc => {
          const im = sc.parentElement.querySelector('img');
          if (!im) return;
          // Must still CONTAIN "prefers-color-scheme" or the check's own
          // selector stops matching and the case reports a false MISSED.
          sc.setAttribute('media', '(prefers-color-scheme: dark) and (min-width: 99999px)');
          im.replaceWith(im.cloneNode(true));
        });
        swap.forEach(im => { im.removeAttribute('srcset'); im.src = im.getAttribute('data-light'); });
        if (pair) {
          document.querySelectorAll('.only-dark').forEach(e => e.style.setProperty('display', 'none', 'important'));
          document.querySelectorAll('.only-light').forEach(e => e.style.setProperty('display', 'block', 'important'));
        }
        return true;
      }
      if (which === 'off-origin') {
        // A pixel to a host that certainly is not this test server. It never
        // has to load — the request leaving is the defect.
        const im = document.createElement('img');
        im.src = 'https://cdn.example.invalid/pixel.png';
        im.alt = '';
        document.body.appendChild(im);
        return true;
      }
      return false;
    }, name);

    if (!applicable) {
      console.log(`  n/a      ${name} — this page does not use that mechanism`);
      await ctx.close();
      continue;
    }

    await page.waitForTimeout(350);
    const r = await inspect(page, { isDark: true });
    const caught =
      (name === 'img-distorted' && r.imgs.some(i => i.skewPct > 1.5)) ||
      (name === 'text-too-small' && r.smallText.length > 0) ||
      (name === 'h-overflow' && (r.overflow.docW > r.overflow.winW + 1 || r.overflow.bleed.length > 0)) ||
      (name === 'dead-anchor' && r.deadAnchors.length > 0) ||
      (name === 'mark-unresolved' && r.marks.some(m => !m.resolved || m.shapes === 0)) ||
      (name === 'both-themes-visible' && r.hiddenPairs.some(p => p.light > 0 && p.dark > 0)) ||
      (name === 'screenshot-wrong-theme' &&
        (r.themedShots.length > 0 || r.hiddenPairs.some(p => p.light > 0 && p.dark === 0))) ||
      (name === 'off-origin' && sawOffOrigin.size > 0);
    console.log(`  ${caught ? 'caught  ' : 'MISSED  '} ${name}`);
    if (!caught) allCaught = false;
    await ctx.close();
  }
  return allCaught;
}

// ---------------------------------------------------------------------------
async function main() {
  if (!existsSync(join(SITE, 'index.html'))) {
    console.error(`check-render: no site/index.html under ${SITE}`);
    process.exit(2);
  }
  const { chromium } = await loadPlaywright();
  const server = await serve(SITE);
  const base = `http://127.0.0.1:${server.address().port}`;
  const browser = await chromium.launch({ headless: true });

  try {
    if (process.argv.includes('--selftest')) {
      console.log('check-render self-test — each invariant is broken on purpose:\n');
      const ok = await selftest(browser, base);
      console.log(ok ? '\nSELF-TEST PASS — every applicable check discriminates.'
        : '\nSELF-TEST FAIL — a check did not notice its own defect.');
      process.exitCode = ok ? 0 : 1;
      return;
    }

    let sampled = 0, marks = 0;
    for (const vp of VIEWPORTS) {
      for (const theme of ['light', 'dark']) {
        for (const path of PAGES) {
          if (!existsSync(join(SITE, path))) continue;
          const r = await checkPage(browser, base, path, theme, vp);
          sampled += r.imgs.length;
          marks += r.marks.length;
        }
      }
    }
    await checkCrossPageAnchors(browser, base);

    console.log(`\nchecked ${VIEWPORTS.length} viewports × 2 themes × ${PAGES.length} pages; ` +
      `${sampled} rendered images and ${marks} symbol stamps measured\n`);
    notes.forEach(n => console.log('  · ' + n));

    if (findings.length) {
      console.error(`\ncheck-render: ${findings.length} finding(s)\n`);
      const byCheck = {};
      findings.forEach(f => (byCheck[f.check] ||= []).push(f));
      for (const [check, list] of Object.entries(byCheck)) {
        console.error(`  ${check} (${list.length})`);
        // Collapse the viewport dimension: the same defect at eight widths is
        // one defect, and printing it eight times buries the others.
        const seen = new Set();
        list.forEach(f => {
          if (seen.has(f.detail)) return;
          seen.add(f.detail);
          console.error(`    ${f.where}\n      ${f.detail}`);
        });
      }
      process.exitCode = 1;
    } else {
      console.log('\ncheck-render: clean');
    }
  } finally {
    await browser.close();
    server.close();
  }
}

main().catch(e => { console.error(e); process.exit(2); });

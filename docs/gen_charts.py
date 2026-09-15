#!/usr/bin/env python3
"""Regenerate the SVG charts in docs/img from the numbers in README.md / RESULTS.md.
Dependency-free (hand-written SVG) so the figures are reproducible anywhere."""
import os, textwrap
def esc(t): return str(t).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
def para(x, y, text, width=95, color=None, size=12, lh=17):
    """Wrapped muted paragraph."""
    col = color or MUTED
    return "".join(f"<text x='{x}' y='{y+i*lh}' fill='{col}' font-size='{size}'>{esc(line)}</text>" for i, line in enumerate(textwrap.wrap(text, width)))
OUT = os.path.join(os.path.dirname(__file__), "img")
FONT = "font-family='JetBrains Mono, SFMono-Regular, Menlo, monospace'"
INK, MUTED, GRID = "#1f2328", "#6a737d", "#d0d7de"
OURS, OTHER = "#c0392b", "#8fa3b8"

def hbars(name, title, rows, unit="ns per message, lower is better", width=720):
    """rows: [(label, value, highlight)] -> horizontal bar chart."""
    rowh, top, left, right = 34, 54, 250, 70
    h = top + rowh * len(rows) + 30
    vmax = max(v for _, v, _ in rows)
    scale = (width - left - right) / vmax
    s = [f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 {width} {h}' width='{width}' height='{h}' {FONT} font-size='13'>",
         f"<rect width='{width}' height='{h}' fill='white'/>",
         f"<text x='16' y='24' font-size='15' font-weight='bold' fill='{INK}'>{esc(title)}</text>",
         f"<text x='16' y='42' fill='{MUTED}'>{esc(unit)}</text>"]
    for i, (label, v, hi) in enumerate(rows):
        y = top + i * rowh
        col = OURS if hi else OTHER
        s.append(f"<text x='{left-10}' y='{y+18}' text-anchor='end' fill='{INK}' font-weight='{'bold' if hi else 'normal'}'>{esc(label)}</text>")
        s.append(f"<rect x='{left}' y='{y+4}' width='{max(2, v*scale):.1f}' height='{rowh-12}' fill='{col}' rx='2'/>")
        s.append(f"<text x='{left + v*scale + 6:.1f}' y='{y+18}' fill='{INK}'>{v:g}</text>")
    s.append("</svg>")
    open(os.path.join(OUT, name), "w").write("\n".join(s))

def grouped(name, title, groups, series, unit="ns per message, lower is better", width=720):
    """groups: [group label]; series: [(name, [values per group], highlight)] -> vertical grouped bars."""
    top, bottom, left, right = 58, 70, 60, 20
    h = 360
    plot_h = h - top - bottom
    vmax = max(v for _, vals, _ in series for v in vals)
    n, m = len(groups), len(series)
    gw = (width - left - right) / n
    bw = gw / (m + 1)
    s = [f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 {width} {h}' width='{width}' height='{h}' {FONT} font-size='12'>",
         f"<rect width='{width}' height='{h}' fill='white'/>",
         f"<text x='16' y='24' font-size='15' font-weight='bold' fill='{INK}'>{esc(title)}</text>",
         f"<text x='16' y='42' fill='{MUTED}'>{esc(unit)}</text>"]
    step = 100 if vmax > 300 else 50
    for g in range(0, int(vmax) + step, step):
        y = top + plot_h - g / vmax * plot_h
        if y < top: break
        s.append(f"<line x1='{left}' x2='{width-right}' y1='{y:.1f}' y2='{y:.1f}' stroke='{GRID}'/>")
        s.append(f"<text x='{left-6}' y='{y+4:.1f}' text-anchor='end' fill='{MUTED}'>{g}</text>")
    for gi, glabel in enumerate(groups):
        x0 = left + gi * gw + bw / 2
        for si, (sname, vals, hi) in enumerate(series):
            v = vals[gi]
            bh = v / vmax * plot_h
            x = x0 + si * bw
            s.append(f"<rect x='{x:.1f}' y='{top+plot_h-bh:.1f}' width='{bw-3:.1f}' height='{bh:.1f}' fill='{OURS if hi else OTHER}' opacity='{1 if hi else 0.55 + 0.15*si}' rx='2'/>")
            s.append(f"<text x='{x + (bw-3)/2:.1f}' y='{top+plot_h-bh-4:.1f}' text-anchor='middle' fill='{INK}' font-size='11'>{v:g}</text>")
        s.append(f"<text x='{left + gi*gw + gw/2:.1f}' y='{top+plot_h+18}' text-anchor='middle' fill='{INK}'>{esc(glabel)}</text>")
    lx = left
    for si, (sname, _, hi) in enumerate(series):
        s.append(f"<rect x='{lx}' y='{h-26}' width='12' height='12' fill='{OURS if hi else OTHER}' opacity='{1 if hi else 0.55 + 0.15*si}'/>")
        s.append(f"<text x='{lx+16}' y='{h-16}' fill='{INK}'>{esc(sname)}</text>")
        lx += 18 + 8 * len(sname) + 20
    s.append("</svg>")
    open(os.path.join(OUT, name), "w").write("\n".join(s))

# 1. SPSC on one Zen 4 box (README machine 3: Ryzen 9 7950X, 8 cores of one CCD)
hbars("spsc-zen4.svg", "One message through the channel, SPSC 1→1 unbounded, Ryzen 9 7950X",
      [("rapidfire", 5.4, True), ("crossbeam SegQueue", 14.5, False), ("std mpsc", 14.8, False),
       ("async-channel", 40.7, False), ("flume", 104.7, False), ("tokio mpsc", 106.0, False)])

# 2. 40 WebSocket readers -> one writer, tokio, Ryzen 9 7900 (README real-bot table, async)
grouped("collector-async-zen4.svg", "40 reader tasks → one writer, unbounded, tokio (4 workers), Ryzen 9 7900",
        ["64-byte payload", "256-byte payload", "1024-byte payload"],
        [("rapidfire", [43, 58, 127], True), ("async-channel", [92, 71, 189], False),
         ("tokio mpsc", [104, 135, 367], False), ("flume", [132, 215, 642], False)])

# 3. rapidfire vs the best other channel per machine, SPSC unbounded (README summary table)
hbars("spsc-ratio-fleet.svg", "SPSC 1→1 unbounded: speed-up over the best other channel, per machine",
      [("Ryzen 9 7900 (Zen 4) #1", 2.53, True), ("Ryzen 9 7900 (Zen 4) #2", 2.44, True),
       ("Ryzen 9 7950X (Zen 4)", 2.69, True), ("Ryzen 9 7950X (Zen 4) #2", 2.41, True),
       ("Ryzen 9 7950X (loaded)", 2.64, True), ("Ryzen 9 7950X3D", 2.46, True), ("Ryzen 9 7950X3D #2", 2.59, True),
       ("Ryzen 9 9950X (Zen 5)", 6.51, True), ("Ryzen 9 3950X (Zen 2)", 6.91, True),
       ("Xeon E5-2699 v3 (Haswell)", 1.75, True), ("Ampere Altra (Neoverse-N1)", 1.33, True),
       ("Apple M3 Pro", 0.88, False)],
      unit="× versus the fastest of crossbeam, std, flume, async-channel, tokio; below 1 = lost")

# 4. perf: instructions per 2M messages, SPSC, Zen 4
hbars("perf-instructions.svg", "Why SPSC is faster: work per 2 M messages, Ryzen 9 7950X (perf stat)",
      [("rapidfire, instructions", 213, True), ("SegQueue, instructions", 372, False),
       ("rapidfire, cycles", 98, True), ("SegQueue, cycles", 218, False)],
      unit="millions; both queues miss L1d equally often (2.3 M); the gap is instruction count")

# 5. HFT pipeline diagram
def box(x, y, w, h, text, sub=None, fill="white", stroke=INK):
    t = f"<rect x='{x}' y='{y}' width='{w}' height='{h}' rx='6' fill='{fill}' stroke='{stroke}' stroke-width='1.5'/>"
    t += f"<text x='{x+w/2}' y='{y+h/2 + (-2 if sub else 5)}' text-anchor='middle' fill='{INK}' font-weight='bold'>{esc(text)}</text>"
    if sub: t += f"<text x='{x+w/2}' y='{y+h/2+14}' text-anchor='middle' fill='{MUTED}' font-size='11'>{esc(sub)}</text>"
    return t
def arrow(x1, y1, x2, y2, label=None):
    t = f"<line x1='{x1}' y1='{y1}' x2='{x2}' y2='{y2}' stroke='{INK}' stroke-width='1.5' marker-end='url(#a)'/>"
    if label: t += f"<text x='{(x1+x2)/2}' y='{min(y1,y2)-6}' text-anchor='middle' fill='{MUTED}' font-size='11'>{esc(label)}</text>"
    return t
W, H = 760, 330
s = [f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 {W} {H}' width='{W}' height='{H}' {FONT} font-size='12'>",
     f"<defs><marker id='a' markerWidth='8' markerHeight='8' refX='7' refY='4' orient='auto'><path d='M0,0 L8,4 L0,8 z' fill='{INK}'/></marker></defs>",
     f"<rect width='{W}' height='{H}' fill='white'/>",
     f"<text x='16' y='24' font-size='15' font-weight='bold' fill='{INK}'>Where the channel sits in a trading process</text>"]
for i, ex in enumerate(["exchange A", "exchange B", "exchange C"]):
    y = 60 + i * 60
    s.append(box(20, y, 110, 40, ex, "WebSocket feed"))
    s.append(box(170, y, 130, 40, f"reader task {i+1}", "parse, timestamp"))
    s.append(arrow(130, y+20, 170, y+20))
    s.append(arrow(300, y+20, 350, 140 if i == 1 else (120 if i == 0 else 160)))
s.append(box(350, 100, 110, 80, "channel", "MPMC, lock-free", fill="#fdecea", stroke=OURS))
s.append(arrow(460, 140, 500, 140))
s.append(box(500, 110, 140, 60, "strategy loop", "one pinned thread"))
s.append(arrow(640, 140, 670, 140))
s.append(box(670, 110, 80, 60, "orders", "gateway"))
s.append(para(20, 250, "Budget: exchange to decision is tens of microseconds end to end, most of it network and parsing. The hop through the channel is one of the few pieces fully under our control: about 5 ns per message on Zen 4 in the one-reader case, no lock and no syscall on the hot path, and the same code when forty readers feed one consumer.", 100))
s.append("</svg>")
open(os.path.join(OUT, "pipeline.svg"), "w").write("\n".join(s))

# 6. Queue layout diagram
W, H = 760, 420
s = [f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 {W} {H}' width='{W}' height='{H}' {FONT} font-size='12'>",
     f"<defs><marker id='a' markerWidth='8' markerHeight='8' refX='7' refY='4' orient='auto'><path d='M0,0 L8,4 L0,8 z' fill='{INK}'/></marker></defs>",
     f"<rect width='{W}' height='{H}' fill='white'/>",
     f"<text x='16' y='24' font-size='15' font-weight='bold' fill='{INK}'>Queue layout: a linked list of 63-slot blocks, three cache lines of indices</text>"]
s.append(box(20, 56, 170, 44, "tail", "producers: index, block", fill="#f6f8fa"))
s.append(box(20, 126, 170, 44, "head", "consumers: index, block", fill="#f6f8fa"))
s.append(box(20, 196, 170, 44, "read marks", "consumer-owned counters", fill="#f6f8fa"))
for b in range(2):
    x = 240 + b * 260
    s.append(f"<rect x='{x}' y='56' width='230' height='150' rx='6' fill='white' stroke='{INK}' stroke-width='1.5'/>")
    s.append(f"<text x='{x+10}' y='76' fill='{INK}' font-weight='bold'>block {b}</text>")
    s.append(f"<text x='{x+80}' y='76' fill='{MUTED}' font-size='11'>{'start = 0, in use' if b == 0 else 'start = 64, spare'}</text>")
    for i in range(7):
        sx = x + 10 + i * 30
        fill = "#fdecea" if (b == 0 and i < 6) else "white"
        s.append(f"<rect x='{sx}' y='88' width='26' height='26' fill='{fill}' stroke='{INK}'/>")
    s.append(f"<text x='{x+10}' y='134' fill='{MUTED}' font-size='11'>… 63 value slots, each with a</text>")
    s.append(f"<text x='{x+10}' y='148' fill='{MUTED}' font-size='11'>lap-tagged state word</text>")
    s.append(f"<rect x='{x+10}' y='160' width='26' height='26' fill='#e6f0ff' stroke='{INK}'/>")
    s.append(f"<text x='{x+44}' y='172' fill='{MUTED}' font-size='11'>slot 63 is the sentinel:</text>")
    s.append(f"<text x='{x+44}' y='185' fill='{MUTED}' font-size='11'>'the next block is installed'</text>")
s.append(arrow(470, 130, 500, 130))
s.append(arrow(190, 78, 240, 78))
s.append(arrow(190, 148, 240, 148))
s.append(f"<text x='240' y='232' fill='{INK}'>written slot</text><rect x='325' y='221' width='14' height='14' fill='#fdecea' stroke='{INK}'/>")
s.append(f"<text x='360' y='232' fill='{INK}'>empty slot</text><rect x='434' y='221' width='14' height='14' fill='white' stroke='{INK}'/>")
s.append(f"<text x='470' y='232' fill='{INK}'>sentinel</text><rect x='530' y='221' width='14' height='14' fill='#e6f0ff' stroke='{INK}'/>")
s.append(para(20, 268, "Each index lives on its own 128-byte line, so a producer never touches the consumers' line on the fast path and vice versa. Producer: one fetch_add on the tail index (a CAS once contention has been seen), write the value, publish the slot state with a release store. Consumer: read the slot state first, then CAS the head index; it never claims a slot that is not written yet. Blocks are never freed while the queue is alive: a spare block and a small pool are recycled, so a slow walker never reads freed memory.", 100))
s.append("</svg>")
open(os.path.join(OUT, "queue-layout.svg"), "w").write("\n".join(s))
print("ok", sorted(os.listdir(OUT)))

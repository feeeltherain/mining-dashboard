from __future__ import annotations

TOKENS = {
    "bg": "#F7F4EE",
    "surface": "#FFFCF8",
    "surface_alt": "#F3EEE6",
    "ink": "#1E2732",
    "muted": "#667180",
    "border": "#E4DDD2",
    "grid": "#ECE5DA",
    "accent": "#355C7D",
    "mine": "#2F5D50",
    "ore": "#B86C25",
    "plant": "#1F7A8C",
    "recovery": "#5B8E7D",
    "metal": "#A14A3B",
    "diesel": "#8A6B4A",
    "good": "#2E7D5A",
    "bad": "#C4584E",
    "warn": "#B88746",
    "shadow": "rgba(21, 30, 38, 0.06)",
}

GROUP_COLORS = {
    "Excavators": TOKENS["mine"],
    "Trucks": TOKENS["accent"],
    "Drills": TOKENS["ore"],
    "Ancillary": "#6D597A",
    "Support Units": TOKENS["diesel"],
}


def rgba(hex_color: str, alpha: float) -> str:
    hex_color = hex_color.lstrip("#")
    r = int(hex_color[0:2], 16)
    g = int(hex_color[2:4], 16)
    b = int(hex_color[4:6], 16)
    return f"rgba({r}, {g}, {b}, {alpha})"


APP_CSS = f"""
<style>
    .stApp {{
        background: linear-gradient(180deg, {TOKENS['bg']} 0%, #F5F1E9 100%);
    }}
    .block-container {{
        max-width: 1500px;
        padding-top: 1.2rem;
        padding-bottom: 3rem;
    }}
    .hero-shell {{
        background: linear-gradient(135deg, {rgba(TOKENS['surface'], 0.98)} 0%, {rgba(TOKENS['surface_alt'], 0.98)} 100%);
        border: 1px solid {TOKENS['border']};
        border-radius: 24px;
        padding: 1.2rem 1.25rem 1rem 1.25rem;
        box-shadow: 0 18px 36px {TOKENS['shadow']};
        margin-bottom: 1rem;
    }}
    .hero-title {{
        color: {TOKENS['ink']};
        font-size: 2rem;
        line-height: 1.05;
        font-weight: 700;
        letter-spacing: -0.04em;
        margin-bottom: 0.18rem;
    }}
    .hero-subtitle {{
        color: {TOKENS['muted']};
        font-size: 0.98rem;
        margin-bottom: 0.9rem;
    }}
    .chip-row {{
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
        margin-bottom: 0.7rem;
    }}
    .chip {{
        display: inline-flex;
        align-items: center;
        padding: 0.42rem 0.75rem;
        border-radius: 999px;
        border: 1px solid {TOKENS['border']};
        background: {TOKENS['surface']};
        color: {TOKENS['ink']};
        font-size: 0.88rem;
        font-weight: 600;
    }}
    .health-chip-good {{ background: {rgba(TOKENS['good'], 0.10)}; border-color: {rgba(TOKENS['good'], 0.18)}; }}
    .health-chip-warn {{ background: {rgba(TOKENS['warn'], 0.12)}; border-color: {rgba(TOKENS['warn'], 0.18)}; }}
    .health-chip-bad {{ background: {rgba(TOKENS['bad'], 0.10)}; border-color: {rgba(TOKENS['bad'], 0.18)}; }}
    .range-banner {{
        display: flex;
        flex-wrap: wrap;
        justify-content: space-between;
        align-items: center;
        gap: 0.7rem;
        background: {TOKENS['ink']};
        color: {TOKENS['surface']};
        border-radius: 18px;
        padding: 0.82rem 0.95rem;
        margin-top: 0.35rem;
    }}
    .range-label {{
        font-size: 0.76rem;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        opacity: 0.74;
    }}
    .range-value {{
        font-size: 1.05rem;
        font-weight: 700;
    }}
    .range-days {{
        display: inline-flex;
        align-items: center;
        padding: 0.32rem 0.62rem;
        border-radius: 999px;
        background: rgba(255,255,255,0.11);
        font-size: 0.82rem;
        font-weight: 600;
    }}
    .readout-shell, .status-shell {{
        background: {rgba(TOKENS['surface'], 0.92)};
        border: 1px solid {TOKENS['border']};
        border-radius: 18px;
        padding: 0.95rem 1rem;
        margin-bottom: 0.9rem;
    }}
    .section-kicker {{
        color: {TOKENS['muted']};
        font-size: 0.76rem;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        margin-bottom: 0.18rem;
    }}
    .readout-text {{
        color: {TOKENS['ink']};
        font-size: 1rem;
        line-height: 1.45;
    }}
    .metric-shell {{
        background: {rgba(TOKENS['surface'], 0.96)};
        border: 1px solid {TOKENS['border']};
        border-radius: 18px;
        padding: 0.95rem 1rem 0.55rem 1rem;
        min-height: 175px;
    }}
    .metric-title {{
        color: {TOKENS['muted']};
        font-size: 0.79rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-bottom: 0.32rem;
    }}
    .metric-value {{
        color: {TOKENS['ink']};
        font-size: 1.72rem;
        font-weight: 700;
        letter-spacing: -0.03em;
        line-height: 1.08;
        margin-bottom: 0.28rem;
    }}
    .metric-delta {{
        font-size: 0.9rem;
        font-weight: 600;
        margin-bottom: 0.12rem;
    }}
    .metric-note {{
        color: {TOKENS['muted']};
        font-size: 0.82rem;
    }}
    .group-heading {{
        color: {TOKENS['ink']};
        font-size: 1.02rem;
        font-weight: 700;
        margin-top: 0.25rem;
        margin-bottom: 0.45rem;
    }}
    .change-pill {{
        display: inline-flex;
        align-items: center;
        margin-right: 0.5rem;
        margin-bottom: 0.45rem;
        border-radius: 999px;
        border: 1px solid {TOKENS['border']};
        padding: 0.4rem 0.72rem;
        background: {TOKENS['surface']};
        color: {TOKENS['ink']};
        font-size: 0.86rem;
        font-weight: 600;
    }}
    .change-pill-good {{
        background: {rgba(TOKENS['good'], 0.10)};
        border-color: {rgba(TOKENS['good'], 0.18)};
    }}
    .change-pill-bad {{
        background: {rgba(TOKENS['bad'], 0.10)};
        border-color: {rgba(TOKENS['bad'], 0.18)};
    }}
    .change-pill-neutral {{
        background: {rgba(TOKENS['accent'], 0.08)};
        border-color: {rgba(TOKENS['accent'], 0.15)};
    }}
    .subtle-lead {{
        color: {TOKENS['muted']};
        font-size: 0.9rem;
        margin-bottom: 0.7rem;
    }}
    .section-divider {{
        height: 0.7rem;
    }}
    .utility-note {{
        color: {TOKENS['muted']};
        font-size: 0.85rem;
    }}
</style>
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import polars as pl

DATA_PATH = Path("data/raw/raia_censo.ndjson")
OUTPUT_PATH = Path("index.html")

SEX_LABELS = {
    "HEMBRA": "Hembra",
    "MACHO": "Macho",
    "DESCONOCIDO": "Desconocido",
}

SIZE_ORDER = [
    "<2 Kg",
    "2-5 Kg",
    "5-10 Kg",
    "10-20 Kg",
    "20-40 Kg",
    ">40 Kg",
    "Desconocido",
]


def collect(lazy_frame: pl.LazyFrame) -> pl.DataFrame:
    return lazy_frame.collect(engine="streaming")


def format_int(value: int | None) -> str:
    if value is None:
        return "N/D"
    return f"{value:,}".replace(",", ".")


def format_date(value: dt.date | str | None) -> str:
    if value is None:
        return "N/D"
    if isinstance(value, str):
        return value
    return value.isoformat()


def clean_text(column: str, unknown: str = "Desconocido") -> pl.Expr:
    return (
        pl.when(pl.col(column).is_null() | (pl.col(column) == ""))
        .then(pl.lit(unknown))
        .otherwise(pl.col(column))
        .alias(column)
    )


def build_overall_stats(scan: pl.LazyFrame, current_year: int) -> dict[str, object]:
    ident_date = pl.col("fechaIdentificacion").str.strptime(pl.Date, strict=False)
    valid_ident_date = pl.when(
        ident_date.dt.year().is_between(2005, current_year)
    ).then(ident_date)
    stats = collect(
        scan.select(
            total=pl.len(),
            species=pl.col("especie").n_unique(),
            provinces=pl.col("provincia").n_unique(),
            municipalities=pl.col("municipio").n_unique(),
            breeds=pl.col("raza").n_unique(),
            max_ident_date=valid_ident_date.max().dt.strftime("%Y-%m-%d"),
        )
    )
    return stats.to_dicts()[0]


def build_species_counts(scan: pl.LazyFrame) -> pl.DataFrame:
    return collect(scan.group_by("especie").agg(pl.len().alias("total"))).sort(
        "total", descending=True
    )


def build_gender_chart(scan: pl.LazyFrame, top_species: list[str]) -> dict[str, list]:
    sexo = (
        pl.when(pl.col("sexo").is_null() | (pl.col("sexo") == ""))
        .then(pl.lit("DESCONOCIDO"))
        .otherwise(pl.col("sexo"))
        .alias("sexo")
    )
    gender_counts = collect(
        scan.with_columns(sexo)
        .filter(pl.col("especie").is_in(top_species))
        .group_by(["especie", "sexo"])
        .agg(pl.len().alias("total"))
    )
    value_map = {
        (row["especie"], row["sexo"]): row["total"]
        for row in gender_counts.to_dicts()
    }
    sexes = ["HEMBRA", "MACHO", "DESCONOCIDO"]
    series = []
    for sex in sexes:
        series.append(
            {
                "name": SEX_LABELS[sex],
                "values": [value_map.get((species, sex), 0) for species in top_species],
            }
        )
    return {"categories": top_species, "series": series}


def build_top_list(
    scan: pl.LazyFrame,
    column: str,
    limit: int,
    unknown_label: str = "Desconocido",
) -> list[dict[str, object]]:
    df = collect(
        scan.with_columns(clean_text(column, unknown_label))
        .group_by(column)
        .agg(pl.len().alias("total"))
        .sort("total", descending=True)
        .head(limit)
    )
    return [{"label": row[column], "value": row["total"]} for row in df.to_dicts()]


def build_sex_distribution(scan: pl.LazyFrame) -> list[dict[str, object]]:
    sexo = (
        pl.when(pl.col("sexo").is_null() | (pl.col("sexo") == ""))
        .then(pl.lit("DESCONOCIDO"))
        .otherwise(pl.col("sexo"))
        .alias("sexo")
    )
    df = collect(scan.with_columns(sexo).group_by("sexo").agg(pl.len().alias("total")))
    value_map = {row["sexo"]: row["total"] for row in df.to_dicts()}
    return [
        {"label": SEX_LABELS[sex], "value": value_map.get(sex, 0)}
        for sex in ["HEMBRA", "MACHO", "DESCONOCIDO"]
    ]


def build_size_distribution(scan: pl.LazyFrame) -> list[dict[str, object]]:
    df = collect(
        scan.with_columns(clean_text("tamano"))
        .group_by("tamano")
        .agg(pl.len().alias("total"))
    )
    value_map = {row["tamano"]: row["total"] for row in df.to_dicts()}
    return [
        {"label": size, "value": value_map.get(size, 0)} for size in SIZE_ORDER
    ]


def build_species_section(scan: pl.LazyFrame, species: str) -> dict[str, object]:
    species_scan = scan.filter(pl.col("especie") == species)
    stats = collect(
        species_scan.select(total=pl.len(), breeds=pl.col("raza").n_unique())
    ).to_dicts()[0]
    return {
        "total": stats["total"],
        "breeds": stats["breeds"],
        "top_breeds": build_top_list(species_scan, "raza", 10),
        "coat_colors": build_top_list(species_scan, "capa", 8),
        "sex": build_sex_distribution(species_scan),
        "size": build_size_distribution(species_scan),
    }


def build_line_charts(
    scan: pl.LazyFrame,
    year_min: int,
    year_max: int,
    top_species: int = 3,
) -> dict[str, object]:
    ident_year = pl.col("fechaIdentificacion").str.strptime(pl.Date, strict=False).dt.year()
    line_df = collect(
        scan.with_columns(year=ident_year)
        .filter(pl.col("year").is_between(year_min, year_max))
        .filter(pl.col("provincia").is_not_null() & (pl.col("provincia") != ""))
        .filter(pl.col("especie").is_not_null() & (pl.col("especie") != ""))
        .group_by(["provincia", "especie", "year"])
        .agg(pl.len().alias("total"))
    )
    species_totals = (
        line_df.group_by(["provincia", "especie"])
        .agg(pl.col("total").sum().alias("total"))
        .sort(["provincia", "total"], descending=[False, True])
    )
    top_species_df = species_totals.group_by("provincia").head(top_species)
    province_totals = (
        species_totals.group_by("provincia")
        .agg(pl.col("total").sum().alias("total"))
        .sort("total", descending=True)
    )
    provinces = province_totals["provincia"].to_list()
    years = list(range(year_min, year_max + 1))
    value_map = {
        (row["provincia"], row["especie"], row["year"]): row["total"]
        for row in line_df.to_dicts()
    }
    top_species_map: dict[str, list[str]] = {}
    for row in top_species_df.to_dicts():
        top_species_map.setdefault(row["provincia"], []).append(row["especie"])

    province_series = []
    for province in provinces:
        species_list = top_species_map.get(province, [])
        series = []
        for species in species_list:
            series.append(
                {
                    "name": species,
                    "values": [
                        value_map.get((province, species, year), 0) for year in years
                    ],
                }
            )
        province_series.append(
            {"province": province, "species": species_list, "series": series}
        )
    return {"years": years, "provinces": province_series}


def build_html(data: dict[str, object]) -> str:
    data_json = json.dumps(data, ensure_ascii=False)
    overall = data["overall"]
    dogs = data["dogs"]
    cats = data["cats"]
    stats_cards = [
        ("Animales", format_int(overall["total"])),
        ("Especies", format_int(overall["species"])),
        ("Razas", format_int(overall["breeds"])),
        ("Provincias", format_int(overall["provinces"])),
        ("Municipios", format_int(overall["municipalities"])),
        ("Última identificación", format_date(overall["max_ident_date"])),
    ]
    stats_html = "\n".join(
        f"""
        <div class="card stat-card">
          <div class="stat-label">{label}</div>
          <div class="stat-value">{value}</div>
        </div>
        """.strip()
        for label, value in stats_cards
    )
    dog_stats = f"{format_int(dogs['total'])} perros y {format_int(dogs['breeds'])} razas"
    cat_stats = f"{format_int(cats['total'])} gatos y {format_int(cats['breeds'])} razas"

    return f"""<!DOCTYPE html>
<html lang="es">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Censo RAIA</title>
    <link rel="icon" href="data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64' fill='%232563eb'><circle cx='20' cy='18' r='6'/><circle cx='44' cy='18' r='6'/><circle cx='14' cy='36' r='6'/><circle cx='50' cy='36' r='6'/><path d='M20 46c0-7 6-12 12-12s12 5 12 12c0 8-7 12-12 12s-12-4-12-12z'/></svg>">
    <style>
      :root {{ color-scheme: light }}
      :root {{ --bg: #f1f5f9 }}
      :root {{ --panel: #ffffff }}
      :root {{ --text: #0f172a }}
      :root {{ --muted: #475569 }}
      :root {{ --border: #e2e8f0 }}
      :root {{ --accent: #2563eb }}
      :root {{ --accent-soft: #dbeafe }}
      * {{ box-sizing: border-box }}
      body {{ margin: 0 }}
      body {{ font-family: "Inter", "Segoe UI", system-ui, sans-serif }}
      body {{ background: var(--bg) }}
      body {{ color: var(--text) }}
      body {{ line-height: 1.5 }}
      main {{ max-width: 1200px }}
      main {{ margin: 0 auto }}
      main {{ padding: 32px 24px 72px }}
      header {{ display: grid }}
      header {{ gap: 8px }}
      header {{ margin-bottom: 32px }}
      h1 {{ font-size: 34px }}
      h1 {{ letter-spacing: -0.02em }}
      h1 {{ margin: 0 }}
      h2 {{ font-size: 22px }}
      h2 {{ margin: 0 }}
      h3 {{ font-size: 14px }}
      h3 {{ margin: 0 }}
      p {{ margin: 0 }}
      p {{ color: var(--muted) }}
      section {{ margin-bottom: 36px }}
      .stats-grid {{ display: grid }}
      .stats-grid {{ grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)) }}
      .stats-grid {{ gap: 16px }}
      .card {{ background: var(--panel) }}
      .card {{ border: 1px solid var(--border) }}
      .card {{ border-radius: 0 }}
      .card {{ padding: 16px }}
      .card {{ box-shadow: none }}
      .stat-card {{ display: grid }}
      .stat-card {{ gap: 6px }}
      .stat-label {{ font-size: 12px }}
      .stat-label {{ text-transform: uppercase }}
      .stat-label {{ letter-spacing: 0.05em }}
      .stat-label {{ color: var(--muted) }}
      .stat-value {{ font-size: 20px }}
      .stat-value {{ font-weight: 600 }}
      .section-header {{ display: flex }}
      .section-header {{ align-items: baseline }}
      .section-header {{ justify-content: space-between }}
      .section-header {{ gap: 16px }}
      .section-note {{ font-size: 14px }}
      .section-note {{ color: var(--muted) }}
      .chart-card {{ display: grid }}
      .chart-card {{ gap: 12px }}
      .chart-title {{ font-size: 12px }}
      .chart-title {{ text-transform: uppercase }}
      .chart-title {{ letter-spacing: 0.04em }}
      .chart-title {{ color: var(--muted) }}
      .province-title {{ font-size: 14px }}
      .province-title {{ font-weight: 600 }}
      .province-title {{ color: var(--text) }}
      .chart {{ width: 100% }}
      .chart {{ min-height: 200px }}
      .chart svg {{ width: 100% }}
      .chart svg {{ height: 100% }}
      .legend {{ display: flex }}
      .legend {{ flex-wrap: wrap }}
      .legend {{ gap: 12px }}
      .legend-item {{ display: flex }}
      .legend-item {{ align-items: center }}
      .legend-item {{ gap: 6px }}
      .legend-swatch {{ width: 12px }}
      .legend-swatch {{ height: 12px }}
      .legend-swatch {{ border-radius: 999px }}
      .legend-label {{ font-size: 13px }}
      .legend-label {{ color: var(--muted) }}
      .grid {{ display: grid }}
      .grid {{ gap: 16px }}
      .grid {{ grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)) }}
      .grid-2x2 {{ display: grid }}
      .grid-2x2 {{ gap: 16px }}
      .grid-2x2 {{ grid-template-columns: repeat(2, minmax(240px, 1fr)) }}
      @media (max-width: 720px) {{
        .grid-2x2 {{ grid-template-columns: 1fr }}
      }}
      .province-list {{ display: grid }}
      .province-list {{ gap: 20px }}
      .axis-label {{ font-size: 11px }}
      .axis-label {{ fill: var(--muted) }}
      .axis-year {{ font-size: 9px }}
      .axis-line {{ stroke: #cbd5f5 }}
      .axis-line {{ stroke-width: 1 }}
      .muted-line {{ stroke: #e2e8f0 }}
      .muted-line {{ stroke-width: 1 }}
      .chart-title-main {{ font-size: 14px }}
      .chart-title-main {{ font-weight: 600 }}
      .footer {{ font-size: 12px }}
      .footer {{ color: var(--muted) }}
      .footer {{ margin-top: 32px }}
      .tooltip {{ position: fixed }}
      .tooltip {{ z-index: 20 }}
      .tooltip {{ padding: 6px 10px }}
      .tooltip {{ background: #0f172a }}
      .tooltip {{ color: #ffffff }}
      .tooltip {{ border-radius: 8px }}
      .tooltip {{ font-size: 12px }}
      .tooltip {{ opacity: 0 }}
      .tooltip {{ transform: translate(-50%, -110%) }}
      .tooltip {{ pointer-events: none }}
      .tooltip {{ transition: opacity 0.12s ease }}
    </style>
  </head>
  <body>
    <main>
      <header>
        <h1>Censo RAIA</h1>
        <p>Datos del censo público RAIA.</p>
      </header>

      <section>
        <div class="stats-grid">
          {stats_html}
        </div>
      </section>

      <section>
        <div class="section-header">
          <h2>Especies por sexo</h2>
          <div class="section-note">Especies principales</div>
        </div>
        <div class="card chart-card">
          <div class="chart" id="species-gender-chart"></div>
          <div class="legend" id="species-gender-legend"></div>
        </div>
      </section>

      <section>
        <div class="section-header">
          <h2>Perros</h2>
          <div class="section-note">{dog_stats}</div>
        </div>
        <div class="grid-2x2">
          <div class="card chart-card">
            <div class="chart-title">Razas principales</div>
            <div class="chart" id="dog-breeds-chart"></div>
          </div>
          <div class="card chart-card">
            <div class="chart-title">Tamaño</div>
            <div class="chart" id="dog-size-chart"></div>
          </div>
          <div class="card chart-card">
            <div class="chart-title">Sexo</div>
            <div class="chart" id="dog-sex-chart"></div>
          </div>
          <div class="card chart-card">
            <div class="chart-title">Colores de capa</div>
            <div class="chart" id="dog-coats-chart"></div>
          </div>
        </div>
      </section>

      <section>
        <div class="section-header">
          <h2>Gatos</h2>
          <div class="section-note">{cat_stats}</div>
        </div>
        <div class="grid-2x2">
          <div class="card chart-card">
            <div class="chart-title">Razas principales</div>
            <div class="chart" id="cat-breeds-chart"></div>
          </div>
          <div class="card chart-card">
            <div class="chart-title">Sexo</div>
            <div class="chart" id="cat-sex-chart"></div>
          </div>
          <div class="card chart-card">
            <div class="chart-title">Tamaño</div>
            <div class="chart" id="cat-size-chart"></div>
          </div>
          <div class="card chart-card">
            <div class="chart-title">Colores de capa</div>
            <div class="chart" id="cat-coats-chart"></div>
          </div>
        </div>
      </section>

      <section>
        <div class="section-header">
          <h2>Altas por provincia</h2>
          <div class="section-note">Top 3 especies por provincia</div>
        </div>
        <div class="province-list" id="province-charts"></div>
      </section>

      <div class="footer">Fuente RAIA.</div>
    </main>

    <div class="tooltip" id="tooltip"></div>

    <script>
      const DATA = {data_json}
      const formatter = new Intl.NumberFormat("es-ES")

      const genderPalette = {{
        Hembra: "#2563eb",
        Macho: "#f97316",
        Desconocido: "#94a3b8"
      }}

      const linePalette = ["#2563eb", "#f97316", "#10b981", "#a855f7"]

      function formatNumber(value) {{
        return formatter.format(value)
      }}

      function truncateLabel(text, max) {{
        if (text.length <= max) {{
          return text
        }}
        return `${{text.slice(0, max - 1)}}…`
      }}

      function createSvg(width, height) {{
        const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg")
        svg.setAttribute("width", width)
        svg.setAttribute("height", height)
        svg.setAttribute("viewBox", `0 0 ${{width}} ${{height}}`)
        return svg
      }}

      function addText(svg, x, y, text, className, anchor, rotate) {{
        const element = document.createElementNS("http://www.w3.org/2000/svg", "text")
        element.setAttribute("x", x)
        element.setAttribute("y", y)
        if (className) {{
          element.setAttribute("class", className)
        }}
        if (anchor) {{
          element.setAttribute("text-anchor", anchor)
        }}
        if (rotate) {{
          element.setAttribute("transform", `rotate(${{rotate}}, ${{x}}, ${{y}})`)
        }}
        element.textContent = text
        svg.appendChild(element)
      }}

      function addLine(svg, x1, y1, x2, y2, className) {{
        const line = document.createElementNS("http://www.w3.org/2000/svg", "line")
        line.setAttribute("x1", x1)
        line.setAttribute("y1", y1)
        line.setAttribute("x2", x2)
        line.setAttribute("y2", y2)
        if (className) {{
          line.setAttribute("class", className)
        }}
        svg.appendChild(line)
      }}

      function showTooltip(event, text) {{
        const tooltip = document.getElementById("tooltip")
        tooltip.textContent = text
        tooltip.style.opacity = "1"
        moveTooltip(event)
      }}

      function moveTooltip(event) {{
        const tooltip = document.getElementById("tooltip")
        tooltip.style.left = `${{event.clientX}}px`
        tooltip.style.top = `${{event.clientY - 12}}px`
      }}

      function hideTooltip() {{
        const tooltip = document.getElementById("tooltip")
        tooltip.style.opacity = "0"
      }}

      function bindTooltip(element, text) {{
        element.addEventListener("mouseenter", (event) => {{
          showTooltip(event, text)
        }})
        element.addEventListener("mousemove", (event) => {{
          moveTooltip(event)
        }})
        element.addEventListener("mouseleave", () => {{
          hideTooltip()
        }})
      }}

      function renderLegend(container, items, palette) {{
        container.innerHTML = ""
        items.forEach((item) => {{
          const legendItem = document.createElement("div")
          legendItem.className = "legend-item"
          const swatch = document.createElement("span")
          swatch.className = "legend-swatch"
          swatch.style.background = palette[item]
          const label = document.createElement("span")
          label.className = "legend-label"
          label.textContent = item
          legendItem.appendChild(swatch)
          legendItem.appendChild(label)
          container.appendChild(legendItem)
        }})
      }}

      function renderStackedBarChart(container, data) {{
        const width = Math.max(container.clientWidth, 320)
        const margin = {{ top: 18, right: 20, bottom: 24, left: 190 }}
        const barHeight = 26
        const gap = 8
        const height =
          margin.top +
          margin.bottom +
          data.categories.length * barHeight +
          (data.categories.length - 1) * gap
        const chartWidth = width - margin.left - margin.right
        const chartHeight = height - margin.top - margin.bottom
        const totals = data.categories.map((_, index) =>
          data.series.reduce((sum, series) => sum + series.values[index], 0)
        )
        const maxValue = Math.max(...totals, 1)
        const svg = createSvg(width, height)

        container.innerHTML = ""
        container.style.height = `${{height}}px`
        container.appendChild(svg)

        const ticks = 4
        const tickValues = Array.from({{ length: ticks }}, (_, index) =>
          Math.round((maxValue / (ticks - 1)) * index)
        )
        tickValues.forEach((value) => {{
          const x = margin.left + (value / maxValue) * chartWidth
          addLine(svg, x, margin.top, x, height - margin.bottom, "muted-line")
          addText(svg, x, height - 6, formatNumber(value), "axis-label", "middle")
        }})

        data.categories.forEach((category, index) => {{
          const y = margin.top + index * (barHeight + gap)
          addText(
            svg,
            margin.left - 8,
            y + barHeight / 2 + 4,
            truncateLabel(category, 22),
            "axis-label",
            "end"
          )
          let x = margin.left
          data.series.forEach((series) => {{
            const value = series.values[index]
            const widthValue = (value / maxValue) * chartWidth
            if (widthValue > 0) {{
              const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect")
              rect.setAttribute("x", x)
              rect.setAttribute("y", y)
              rect.setAttribute("width", widthValue)
              rect.setAttribute("height", barHeight)
              rect.setAttribute("fill", genderPalette[series.name])
              rect.setAttribute("rx", 6)
              rect.setAttribute("ry", 6)
              svg.appendChild(rect)
              bindTooltip(
                rect,
                `${{category}} · ${{series.name}}: ${{formatNumber(value)}}`
              )
              x += widthValue
            }}
          }})
        }})
      }}

      function renderHorizontalBarChart(container, items, color) {{
        const width = Math.max(container.clientWidth, 320)
        const margin = {{ top: 12, right: 20, bottom: 18, left: 220 }}
        const barHeight = 24
        const gap = 8
        const height =
          margin.top +
          margin.bottom +
          items.length * barHeight +
          (items.length - 1) * gap
        const chartWidth = width - margin.left - margin.right
        const chartHeight = height - margin.top - margin.bottom
        const maxValue = Math.max(...items.map((item) => item.value), 1)
        const svg = createSvg(width, height)

        container.innerHTML = ""
        container.style.height = `${{height}}px`
        container.appendChild(svg)

        items.forEach((item, index) => {{
          const y = margin.top + index * (barHeight + gap)
          addText(
            svg,
            margin.left - 8,
            y + barHeight / 2 + 4,
            truncateLabel(item.label, 24),
            "axis-label",
            "end"
          )
          const barWidth = (item.value / maxValue) * chartWidth
          const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect")
          rect.setAttribute("x", margin.left)
          rect.setAttribute("y", y)
          rect.setAttribute("width", barWidth)
          rect.setAttribute("height", barHeight)
          rect.setAttribute("fill", color)
          rect.setAttribute("rx", 6)
          rect.setAttribute("ry", 6)
          svg.appendChild(rect)
          bindTooltip(rect, `${{item.label}}: ${{formatNumber(item.value)}}`)
        }})
      }}

      function renderLineChart(container, chart, palette) {{
        const width = Math.max(container.clientWidth, 320)
        const height = 240
        const margin = {{ top: 16, right: 16, bottom: 48, left: 36 }}
        const chartWidth = width - margin.left - margin.right
        const chartHeight = height - margin.top - margin.bottom
        const maxValue = Math.max(
          ...chart.series.flatMap((series) => series.values),
          1
        )
        const svg = createSvg(width, height)

        container.innerHTML = ""
        container.style.height = `${{height}}px`
        container.appendChild(svg)

        addLine(
          svg,
          margin.left,
          height - margin.bottom,
          width - margin.right,
          height - margin.bottom,
          "axis-line"
        )
        addLine(
          svg,
          margin.left,
          margin.top,
          margin.left,
          height - margin.bottom,
          "axis-line"
        )

        const xScale = (index) =>
          margin.left + (index / (chart.years.length - 1)) * chartWidth
        const yScale = (value) =>
          height - margin.bottom - (value / maxValue) * chartHeight

        const ticks = 4
        const tickValues = Array.from({{ length: ticks }}, (_, index) =>
          Math.round((maxValue / (ticks - 1)) * index)
        )
        tickValues.forEach((value) => {{
          const y = yScale(value)
          addLine(svg, margin.left, y, width - margin.right, y, "muted-line")
          addText(svg, margin.left - 6, y + 4, formatNumber(value), "axis-label", "end")
        }})

        chart.years.forEach((year, index) => {{
          const x = xScale(index)
          addText(svg, x, height - 6, year, "axis-label axis-year", "middle", -45)
        }})

        chart.series.forEach((series) => {{
          const color = palette[series.name]
          const path = document.createElementNS("http://www.w3.org/2000/svg", "path")
          const d = series.values
            .map((value, index) => {{
              const x = xScale(index)
              const y = yScale(value)
              return `${{index === 0 ? "M" : "L"}}${{x}},${{y}}`
            }})
            .join(" ")
          path.setAttribute("d", d)
          path.setAttribute("fill", "none")
          path.setAttribute("stroke", color)
          path.setAttribute("stroke-width", 2)
          svg.appendChild(path)

          series.values.forEach((value, index) => {{
            const x = xScale(index)
            const y = yScale(value)
            const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle")
            circle.setAttribute("cx", x)
            circle.setAttribute("cy", y)
            circle.setAttribute("r", 2.5)
            circle.setAttribute("fill", color)
            circle.setAttribute("opacity", 0.7)
            circle.setAttribute("pointer-events", "all")
            circle.style.cursor = "pointer"
            svg.appendChild(circle)
            circle.addEventListener("mouseenter", (event) => {{
              circle.setAttribute("opacity", 1)
              circle.setAttribute("r", 4)
              showTooltip(
                event,
                `${{series.name}} · ${{chart.years[index]}}: ${{formatNumber(value)}}`
              )
            }})
            circle.addEventListener("mousemove", (event) => {{
              moveTooltip(event)
            }})
            circle.addEventListener("mouseleave", () => {{
              circle.setAttribute("opacity", 0.7)
              circle.setAttribute("r", 2.5)
              hideTooltip()
            }})
          }})
        }})
      }}

      function renderLineCharts(container, data) {{
        container.innerHTML = ""
        data.provinces.forEach((province) => {{
          const card = document.createElement("div")
          card.className = "card chart-card"
          const title = document.createElement("div")
          title.className = "province-title"
          title.textContent = province.province
          const legend = document.createElement("div")
          legend.className = "legend"
          const chartContainer = document.createElement("div")
          chartContainer.className = "chart"
          card.appendChild(title)
          card.appendChild(legend)
          card.appendChild(chartContainer)
          container.appendChild(card)
          const palette = Object.fromEntries(
            province.species.map((species, index) => [species, linePalette[index % linePalette.length]])
          )
          renderLegend(legend, province.species, palette)
          renderLineChart(
            chartContainer,
            {{ years: data.years, series: province.series }},
            palette
          )
        }})
      }}

      function renderAll() {{
        const genderChart = document.getElementById("species-gender-chart")
        renderStackedBarChart(genderChart, DATA.top_species_gender)
        renderLegend(
          document.getElementById("species-gender-legend"),
          DATA.top_species_gender.series.map((series) => series.name),
          genderPalette
        )
        renderHorizontalBarChart(
          document.getElementById("dog-breeds-chart"),
          DATA.dogs.top_breeds,
          "#2563eb"
        )
        renderHorizontalBarChart(
          document.getElementById("dog-size-chart"),
          DATA.dogs.size,
          "#0ea5e9"
        )
        renderHorizontalBarChart(
          document.getElementById("dog-sex-chart"),
          DATA.dogs.sex,
          "#f97316"
        )
        renderHorizontalBarChart(
          document.getElementById("dog-coats-chart"),
          DATA.dogs.coat_colors,
          "#64748b"
        )
        renderHorizontalBarChart(
          document.getElementById("cat-breeds-chart"),
          DATA.cats.top_breeds,
          "#10b981"
        )
        renderHorizontalBarChart(
          document.getElementById("cat-sex-chart"),
          DATA.cats.sex,
          "#f97316"
        )
        renderHorizontalBarChart(
          document.getElementById("cat-size-chart"),
          DATA.cats.size,
          "#0ea5e9"
        )
        renderHorizontalBarChart(
          document.getElementById("cat-coats-chart"),
          DATA.cats.coat_colors,
          "#64748b"
        )
        renderLineCharts(document.getElementById("province-charts"), DATA.line)
      }}

      window.addEventListener("load", () => {{
        renderAll()
      }})

      window.addEventListener("resize", () => {{
        renderAll()
      }})
    </script>
  </body>
</html>
"""


def main() -> None:
    assert DATA_PATH.exists(), f"Missing data file at {DATA_PATH}"
    scan = pl.scan_ndjson(DATA_PATH, infer_schema_length=5000)
    current_year = dt.date.today().year
    overall = build_overall_stats(scan, current_year)
    species_counts = build_species_counts(scan)
    top_species = species_counts.head(10)["especie"].to_list()
    top_species_gender = build_gender_chart(scan, top_species)
    dogs = build_species_section(scan, "PERRO")
    cats = build_species_section(scan, "GATO")
    line_charts = build_line_charts(scan, 2005, current_year)

    data = {
        "generated_at": dt.date.today().isoformat(),
        "overall": overall,
        "top_species_gender": top_species_gender,
        "dogs": dogs,
        "cats": cats,
        "line": line_charts,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(build_html(data), encoding="utf-8")
    print(f"Wrote report to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()

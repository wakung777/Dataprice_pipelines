import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date

st.set_page_config(page_title="Price Intelligence", page_icon="🛒", layout="wide")

DB = dict(host="postgres", dbname="airflow", user="airflow", password="airflow", port=5432)


@st.cache_data(ttl=300)
def query(sql, params=None):
    conn = psycopg2.connect(**DB)
    df = pd.read_sql(sql, conn, params=params)
    conn.close()
    return df


# ── Sidebar ──────────────────────────────────────────────────────────────────
st.sidebar.title("🛒 Price Intelligence")
available_dates = query("SELECT DISTINCT scraped_date FROM product_from_web ORDER BY scraped_date DESC")
if available_dates.empty:
    st.warning("ยังไม่มีข้อมูล — รัน DAG ก่อน")
    st.stop()

dates = available_dates["scraped_date"].tolist()
selected_date = st.sidebar.selectbox("วันที่", dates, format_func=lambda d: str(d))
source_filter = st.sidebar.multiselect("ร้านค้า", ["lotus", "bigc"], default=["lotus", "bigc"])

# ── Load data ─────────────────────────────────────────────────────────────────
df = query(
    "SELECT * FROM product_from_web WHERE scraped_date = %s AND source = ANY(%s) ORDER BY final_price DESC",
    (selected_date, list(source_filter)),
)

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🛒 Price Intelligence Dashboard")
st.caption(f"ข้อมูล: {selected_date} | {len(df)} รายการ")

# ── KPI cards ─────────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
for src, col, icon in [("lotus", col1, "🔵"), ("bigc", col2, "🟢")]:
    sub = df[df["source"] == src]
    if not sub.empty:
        col.metric(f"{icon} {src.upper()} สินค้า", f"{len(sub)} รายการ")
        col.metric(f"ราคาเฉลี่ย", f"฿{sub['final_price'].mean():.0f}")

with col3:
    on_sale = df[df["discount_percent"] > 0]
    st.metric("🏷️ สินค้าที่มีส่วนลด", f"{len(on_sale)} รายการ")
    if not on_sale.empty:
        st.metric("ลดสูงสุด", f"{on_sale['discount_percent'].max():.1f}%")

with col4:
    out = df[df["stock_status"].str.contains("OUT|หมด", case=False, na=False)]
    st.metric("❌ สินค้าหมด", f"{len(out)} รายการ")

st.divider()

# ── Tab layout ────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(["📊 เปรียบราคา", "🏷️ โปรโมชัน", "📈 Trend ราคา", "🤖 AI Insights"])

# ── Tab 1: Price comparison ───────────────────────────────────────────────────
with tab1:
    st.subheader("เปรียบราคา Lotus vs Big C")

    if len(source_filter) == 2:
        lotus_df = df[df["source"] == "lotus"][["name", "final_price", "discount_percent"]].copy()
        bigc_df  = df[df["source"] == "bigc"][["name", "final_price", "discount_percent"]].copy()

        # simple name-based match (first 8 chars)
        lotus_df["key"] = lotus_df["name"].str[:10]
        bigc_df["key"]  = bigc_df["name"].str[:10]
        merged = lotus_df.merge(bigc_df, on="key", suffixes=("_lotus", "_bigc")).head(20)

        if not merged.empty:
            merged["diff"] = merged["final_price_lotus"] - merged["final_price_bigc"]
            merged = merged[merged["diff"].abs() <= 10]
            merged["label"] = merged["name_lotus"].str[:30]
            merged_sorted = merged.sort_values("diff", ascending=True)

            fig = go.Figure()
            fig.add_bar(name="Lotus", x=merged_sorted["label"], y=merged_sorted["final_price_lotus"],
                        marker_color="#3b82f6")
            fig.add_bar(name="Big C", x=merged_sorted["label"], y=merged_sorted["final_price_bigc"],
                        marker_color="#22c55e")
            fig.update_layout(barmode="group", xaxis_tickangle=-45,
                              yaxis_title="ราคา (฿)", height=450)
            st.plotly_chart(fig, use_container_width=True)

            st.subheader("ส่วนต่างราคา (Lotus - Big C)")
            merged_sorted["color"] = merged_sorted["diff"].apply(lambda x: "🔴 Lotus แพงกว่า" if x > 2 else ("🟢 Lotus ถูกกว่า" if x < -2 else "⚪ ใกล้เคียง"))
            st.dataframe(
                merged_sorted[["label", "final_price_lotus", "final_price_bigc", "diff", "color"]]
                .rename(columns={"label": "สินค้า", "final_price_lotus": "Lotus (฿)",
                                  "final_price_bigc": "Big C (฿)", "diff": "ต่าง (฿)", "color": "สถานะ"}),
                use_container_width=True, hide_index=True,
            )
        else:
            st.info("ไม่พบสินค้าที่ match กันได้ในวันนี้")
    else:
        top20 = df.nlargest(20, "final_price")
        fig = px.bar(top20, x="name", y="final_price", color="source",
                     color_discrete_map={"lotus": "#3b82f6", "bigc": "#22c55e"},
                     labels={"final_price": "ราคา (฿)", "name": "สินค้า"}, height=450)
        fig.update_xaxes(tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

# ── Tab 2: Promotions ─────────────────────────────────────────────────────────
with tab2:
    st.subheader("🏷️ สินค้าที่มีส่วนลดสูงสุด Top 15")
    on_sale = df[df["discount_percent"] > 0].nlargest(15, "discount_percent")
    if not on_sale.empty:
        fig = px.bar(
            on_sale, x="discount_percent", y="name", color="source", orientation="h",
            color_discrete_map={"lotus": "#3b82f6", "bigc": "#22c55e"},
            labels={"discount_percent": "ส่วนลด (%)", "name": "สินค้า"},
            text="discount_percent", height=500,
        )
        fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("ราคาก่อน/หลังลด")
        compare = on_sale[["source", "name", "regular_price", "final_price", "discount_percent", "promotion_text"]].copy()
        compare["saving"] = compare["regular_price"] - compare["final_price"]
        st.dataframe(
            compare.rename(columns={
                "source": "ร้าน", "name": "สินค้า", "regular_price": "ปกติ (฿)",
                "final_price": "ขาย (฿)", "discount_percent": "ลด (%)",
                "saving": "ประหยัด (฿)", "promotion_text": "โปรโมชัน",
            }),
            use_container_width=True, hide_index=True,
        )
    else:
        st.info("ไม่มีสินค้าที่มีส่วนลดในวันนี้")

# ── Tab 3: Price trend ────────────────────────────────────────────────────────
with tab3:
    st.subheader("📈 Trend ราคาเฉลี่ยรายวัน")
    trend = query(
        "SELECT source, scraped_date, ROUND(AVG(final_price),2) as avg_price, COUNT(*) as products "
        "FROM product_from_web GROUP BY source, scraped_date ORDER BY scraped_date"
    )
    if len(trend["scraped_date"].unique()) > 1:
        fig = px.line(trend, x="scraped_date", y="avg_price", color="source",
                      color_discrete_map={"lotus": "#3b82f6", "bigc": "#22c55e"},
                      markers=True, labels={"avg_price": "ราคาเฉลี่ย (฿)", "scraped_date": "วันที่"},
                      height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("ต้องมีข้อมูลอย่างน้อย 2 วันถึงจะแสดง trend ได้ — รัน DAG วันพรุ่งนี้แล้วกลับมาดู")

    st.subheader("จำนวนสินค้าที่เก็บได้รายวัน")
    count_trend = query(
        "SELECT source, scraped_date, COUNT(*) as count FROM product_from_web "
        "GROUP BY source, scraped_date ORDER BY scraped_date"
    )
    fig2 = px.bar(count_trend, x="scraped_date", y="count", color="source", barmode="group",
                  color_discrete_map={"lotus": "#3b82f6", "bigc": "#22c55e"},
                  labels={"count": "จำนวนสินค้า", "scraped_date": "วันที่"}, height=350)
    st.plotly_chart(fig2, use_container_width=True)

# ── Tab 4: AI Insights ────────────────────────────────────────────────────────
with tab4:
    st.subheader("🤖 AI Analysis ล่าสุด")
    insights = query(
        "SELECT insight_type, content, created_at FROM ai_answer "
        "ORDER BY created_at DESC LIMIT 10"
    )
    if insights.empty:
        st.info("ยังไม่มี AI insights — รัน DAG ก่อน")
    else:
        latest_analysis = insights[insights["insight_type"] == "pricing_analysis"]
        if not latest_analysis.empty:
            row = latest_analysis.iloc[0]
            st.caption(f"วิเคราะห์เมื่อ: {row['created_at']}")
            st.markdown(row["content"])
            st.divider()

        with st.expander("ดู Product Matches"):
            matches = insights[insights["insight_type"].isin(["product_matches", "product_matches_raw"])]
            if not matches.empty:
                try:
                    import json
                    match_data = pd.DataFrame(json.loads(matches.iloc[0]["content"]))
                    st.dataframe(match_data, use_container_width=True, hide_index=True)
                except Exception:
                    st.text(matches.iloc[0]["content"])

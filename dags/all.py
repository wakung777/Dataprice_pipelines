import os
import json
import logging
from datetime import datetime, date

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

GROQ_URL = 'https://api.groq.com/openai/v1/chat/completions'
GROQ_MODEL = 'llama-3.3-70b-versatile'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 26),
    'retries': 1,
}


def call_groq(prompt):
    api_key = os.environ.get('GROQ_API_KEY')
    resp = requests.post(
        GROQ_URL,
        headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'},
        json={
            'model': GROQ_MODEL,
            'messages': [{'role': 'user', 'content': prompt}],
            'temperature': 0.3,
        },
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()['choices'][0]['message']['content'].strip()


def create_tables():
    hook = PostgresHook(postgres_conn_id='airflow_db')
    hook.run("""
        CREATE TABLE IF NOT EXISTS product_from_web (
            id               SERIAL PRIMARY KEY,
            source           VARCHAR(50)   NOT NULL,
            sku              VARCHAR(255)  NOT NULL,
            name             TEXT          NOT NULL,
            final_price      NUMERIC(10,2),
            scraped_date     DATE          NOT NULL DEFAULT CURRENT_DATE,
            regular_price    NUMERIC(10,2),
            discount_percent NUMERIC(5,2),
            promotion_text   TEXT,
            stock_status     VARCHAR(100),
            unit             VARCHAR(100),
            product_id       VARCHAR(255),
            member_price     NUMERIC(10,2),
            thumbnail_url    TEXT,
            scraped_at       TIMESTAMP     NOT NULL DEFAULT NOW(),
            UNIQUE(source, sku, scraped_date)
        );
        CREATE TABLE IF NOT EXISTS ai_answer (
            id           SERIAL PRIMARY KEY,
            insight_type VARCHAR(100),
            content      TEXT,
            created_at   TIMESTAMP DEFAULT NOW()
        );
    """)
    logging.info("Tables ready.")


def scrape_lotus(**context):
    url = 'https://api-o2o.lotuss.com/lotuss-mobile-bff/product/v4/products'
    params = {'category_id': 91291, 'page': 1, 'limit': 100, 'seller_id': 3}

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    products = resp.json().get('data', {}).get('products', [])

    result = []
    for p in products:
        regular = float(p.get('regularPricePerUOW') or 0)
        final   = float(p.get('finalPricePerUOW')   or regular)
        member  = float(p.get('loyaltyMemberPricePerUOW') or 0)
        discount = round((regular - final) / regular * 100, 2) if regular > 0 else 0.0

        # ดึง promotion text จาก autoBadge ก่อน (มีข้อความจริง เช่น "ซื้อ 3 ชิ้น 145 บาท")
        promo_text = ''
        try:
            badge_items = (
                p.get('autoBadge', {})
                 .get('imagePromotion', {})
                 .get('items', [])
            )
            if badge_items:
                promo_text = badge_items[0]['items'][0].get('offerText', '')
        except (IndexError, KeyError):
            pass
        if not promo_text:
            promotions = p.get('promotions') or []
            raw_text = promotions[0].get('offerText', '') if promotions else ''
            promo_text = raw_text if raw_text != 'Promotion Badge' else ''

        thumbnail = (p.get('thumbnail') or {}).get('url', '')
        unit      = p.get('unitOfQuantity') or p.get('unitOfWeight', '')

        result.append({
            'product_id':       str(p.get('id', '')),
            'sku':              p.get('sku', '') or str(p.get('id', '')),
            'name':             p.get('name', ''),
            'regular_price':    regular,
            'final_price':      final,
            'member_price':     member,
            'discount_percent': discount,
            'stock_status':     p.get('stockStatus', ''),
            'promotion_text':   promo_text,
            'thumbnail_url':    thumbnail,
            'unit':             unit,
        })

    context['ti'].xcom_push(key='lotus_products', value=result)
    logging.info(f"Lotus: scraped {len(result)} products.")


def scrape_bigc(**context):
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage'],
        )
        page = browser.new_page(
            user_agent=(
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/120.0.0.0 Safari/537.36'
            )
        )
        page.goto(
            'https://www.bigc.co.th/category/beverages',
            wait_until='domcontentloaded',
            timeout=60000,
        )
        content = page.content()
        browser.close()

    soup = BeautifulSoup(content, 'lxml')
    script_tag = soup.find('script', id='__NEXT_DATA__')
    if not script_tag:
        raise ValueError("__NEXT_DATA__ not found — Big C page structure may have changed")

    data     = json.loads(script_tag.string)
    products = data['props']['pageProps']['productCategory']['products_summary']['products']

    result = []
    for p in products:
        regular  = float(p.get('price_incl_tax') or 0)
        final    = float(p.get('final_price_incl_tax') or p.get('price_sales') or regular)
        discount = round((regular - final) / regular * 100, 2) if regular > 0 else 0.0

        result.append({
            'product_id':       str(p.get('product_id', '')),
            'sku':              p.get('sku', '') or str(p.get('product_id', '')),
            'name':             p.get('name', ''),
            'regular_price':    regular,
            'final_price':      final,
            'member_price':     0.0,
            'discount_percent': discount,
            'stock_status':     p.get('status', ''),
            'promotion_text':   p.get('label_front_name', ''),
            'thumbnail_url':    p.get('thumbnail_image', ''),
            'unit':             p.get('unit', ''),
        })

    context['ti'].xcom_push(key='bigc_products', value=result)
    logging.info(f"Big C: scraped {len(result)} products.")


def _upsert_products(source, products):
    if not products:
        logging.warning(f"No {source} products to insert.")
        return

    hook = PostgresHook(postgres_conn_id='airflow_db')
    today = date.today().isoformat()
    conn   = hook.get_conn()
    cursor = conn.cursor()

    for p in products:
        cursor.execute(
            """
            INSERT INTO product_from_web (
                source, sku, name,
                final_price, scraped_date,
                regular_price, discount_percent,
                promotion_text, stock_status, unit,
                product_id, member_price, thumbnail_url,
                scraped_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (source, sku, scraped_date)
            DO UPDATE SET
                name             = EXCLUDED.name,
                final_price      = EXCLUDED.final_price,
                regular_price    = EXCLUDED.regular_price,
                discount_percent = EXCLUDED.discount_percent,
                promotion_text   = EXCLUDED.promotion_text,
                stock_status     = EXCLUDED.stock_status,
                unit             = EXCLUDED.unit,
                member_price     = EXCLUDED.member_price,
                thumbnail_url    = EXCLUDED.thumbnail_url,
                scraped_at       = NOW()
            """,
            (
                source, p['sku'], p['name'],
                p['final_price'], today,
                p['regular_price'], p['discount_percent'],
                p['promotion_text'], p['stock_status'], p['unit'],
                p['product_id'], p['member_price'], p['thumbnail_url'],
            ),
        )

    conn.commit()
    cursor.close()
    logging.info(f"{source}: upserted {len(products)} rows.")


def insert_lotus(**context):
    products = context['ti'].xcom_pull(key='lotus_products', task_ids='scrape_lotus')
    _upsert_products('lotus', products)


def insert_bigc(**context):
    products = context['ti'].xcom_pull(key='bigc_products', task_ids='scrape_bigc')
    _upsert_products('bigc', products)


def match_products():
    hook = PostgresHook(postgres_conn_id='airflow_db')

    lotus = hook.get_records(
        "SELECT sku, name, final_price, discount_percent FROM product_from_web "
        "WHERE source='lotus' AND scraped_date=CURRENT_DATE"
    )
    bigc = hook.get_records(
        "SELECT sku, name, final_price, discount_percent FROM product_from_web "
        "WHERE source='bigc' AND scraped_date=CURRENT_DATE"
    )

    if not lotus or not bigc:
        logging.warning(
            f"Not enough data — Lotus: {len(lotus or [])}, Big C: {len(bigc or [])}. Skipping."
        )
        return

    lotus_text = '\n'.join(
        f"- SKU:{r[0]} | {r[1]} | ฿{r[2]} | ลด {r[3]}%" for r in lotus[:30]
    )
    bigc_text = '\n'.join(
        f"- SKU:{r[0]} | {r[1]} | ฿{r[2]} | ลด {r[3]}%" for r in bigc[:30]
    )

    prompt = f"""คุณเป็น AI ที่ช่วยจับคู่สินค้าจากสองซูเปอร์มาร์เก็ต

สินค้า Lotus (สูงสุด 30 รายการ):
{lotus_text}

สินค้า Big C (สูงสุด 30 รายการ):
{bigc_text}

กฎการจับคู่ (สำคัญมาก):
- จับคู่เฉพาะสินค้าที่ **ยี่ห้อเดียวกัน + ปริมาณเท่ากัน + จำนวนชิ้น/ขวดเท่ากัน** เท่านั้น
- ห้ามจับคู่ขวดเดี่ยวกับแพ็ค เช่น "330 มล. x1" ≠ "330 มล. แพ็ค 12"
- ห้ามจับคู่ขนาดต่างกัน เช่น "600 มล." ≠ "1500 มล."
- ถ้าไม่มีคู่ที่ตรงทั้งยี่ห้อ ขนาด และจำนวน → ไม่ต้องจับคู่ ดีกว่าจับผิด
- ใช้ค่า SKU ตามที่ให้มาเท่านั้น ห้ามแต่งขึ้นเอง

ตอบเป็น JSON array เท่านั้น:
[{{"lotus_id":"<lotus SKU>","bigc_id":"<bigc SKU>","product_name":"..."}}]
ห้ามมีข้อความอื่นนอกจาก JSON"""

    result = call_groq(prompt)

    try:
        matches      = json.loads(result)
        content      = json.dumps(matches, ensure_ascii=False)
        insight_type = 'product_matches'
    except json.JSONDecodeError:
        content      = result
        insight_type = 'product_matches_raw'

    hook.run(
        "INSERT INTO ai_answer (insight_type, content, created_at) VALUES (%s, %s, NOW())",
        parameters=(insight_type, content),
    )
    logging.info(f"Saved product matches ({insight_type}).")


def analyze_pricing():
    hook = PostgresHook(postgres_conn_id='airflow_db')

    matches_row = hook.get_first(
        "SELECT content FROM ai_answer "
        "WHERE insight_type='product_matches' ORDER BY created_at DESC LIMIT 1"
    )
    if not matches_row:
        logging.warning("No matched products found. Skipping.")
        return

    try:
        matches = json.loads(matches_row[0])
    except json.JSONDecodeError:
        logging.warning("Could not parse product_matches JSON. Skipping.")
        return

    # ดึงราคาจริงจาก DB — Python join เพื่อไม่ให้ AI เดาราคาผิด
    lotus_prices = {
        r[0]: {
            'name': r[1], 'final_price': float(r[2] or 0),
            'regular_price': float(r[3] or 0), 'discount_percent': float(r[4] or 0),
            'promotion_text': r[5] or '', 'stock_status': r[6] or '',
        }
        for r in hook.get_records(
            "SELECT sku, name, final_price, regular_price, discount_percent, "
            "promotion_text, stock_status FROM product_from_web "
            "WHERE source='lotus' AND scraped_date=CURRENT_DATE"
        )
    }
    bigc_prices = {
        r[0]: {
            'name': r[1], 'final_price': float(r[2] or 0),
            'regular_price': float(r[3] or 0), 'discount_percent': float(r[4] or 0),
            'promotion_text': r[5] or '', 'stock_status': r[6] or '',
        }
        for r in hook.get_records(
            "SELECT sku, name, final_price, regular_price, discount_percent, "
            "promotion_text, stock_status FROM product_from_web "
            "WHERE source='bigc' AND scraped_date=CURRENT_DATE"
        )
    }

    if not lotus_prices or not bigc_prices:
        logging.warning("No price data for today. Skipping.")
        return

    # สร้างตาราง comparison ที่ถูกต้อง 100% ก่อนส่ง AI
    comparison = []
    for m in matches:
        l = lotus_prices.get(m.get('lotus_id', ''))
        b = bigc_prices.get(m.get('bigc_id', ''))
        if not l or not b:
            continue
        diff      = round(l['final_price'] - b['final_price'], 2)
        diff_pct  = round(diff / b['final_price'] * 100, 1) if b['final_price'] else 0
        comparison.append({
            'product':          m.get('product_name', l['name']),
            'lotus_regular':    l['regular_price'],
            'lotus_final':      l['final_price'],
            'lotus_discount':   l['discount_percent'],
            'lotus_promo':      l['promotion_text'],
            'lotus_stock':      l['stock_status'],
            'bigc_regular':     b['regular_price'],
            'bigc_final':       b['final_price'],
            'bigc_discount':    b['discount_percent'],
            'bigc_promo':       b['promotion_text'],
            'bigc_stock':       b['stock_status'],
            'price_diff':       diff,        # บวก = Lotus แพงกว่า, ลบ = Lotus ถูกกว่า
            'diff_pct':         diff_pct,
        })

    if not comparison:
        logging.warning("No comparable products after join. Skipping.")
        return

    comparison.sort(key=lambda x: x['price_diff'], reverse=True)

    table_lines = []
    for c in comparison:
        sign   = f"+฿{c['price_diff']}" if c['price_diff'] > 0 else f"฿{c['price_diff']}"
        l_promo = f" [{c['lotus_promo']}]" if c['lotus_promo'] else ""
        b_promo = f" [{c['bigc_promo']}]" if c['bigc_promo'] else ""
        table_lines.append(
            f"• {c['product']}\n"
            f"  Lotus  : ฿{c['lotus_final']} (ปกติ ฿{c['lotus_regular']}, ลด {c['lotus_discount']}%){l_promo} [{c['lotus_stock']}]\n"
            f"  Big C  : ฿{c['bigc_final']} (ปกติ ฿{c['bigc_regular']}, ลด {c['bigc_discount']}%){b_promo} [{c['bigc_stock']}]\n"
            f"  ผลต่าง : {sign} ({c['diff_pct']}%) {'← Lotus แพงกว่า' if c['price_diff'] > 2 else '← Lotus ถูกกว่า' if c['price_diff'] < -2 else '← ราคาใกล้เคียง'}"
        )

    table = '\n\n'.join(table_lines)

    prompt = f"""คุณคือที่ปรึกษาด้านกลยุทธ์ราคาระดับ Senior ให้กับทีม Price Management ของ Lotus ประเทศไทย

วันที่วิเคราะห์: {date.today().isoformat()}
หมวดสินค้า: เครื่องดื่ม (Beverages)

ตารางเปรียบเทียบราคาระหว่าง Lotus และ Big C (ข้อมูลจริงจากระบบ):

{table}

**กฎการเขียนรายงาน:** ทุกครั้งที่กล่าวถึงสินค้าใด ต้องระบุในรูปแบบ "ชื่อสินค้า (Lotus ฿XX vs Big C ฿XX, ต่าง ฿XX)" เสมอ เพื่อให้ผู้อ่านเห็นภาพได้ทันที 
และอันไหนที่มีคำว่า "แพ็ค" หรือ "ขวด" ต้องระวังมากพิเศษจะไม่เอาไปเทียบกับอันที่ไม่มีคำเหล่านี้ แล้วอันไหนราคาต่างกันเกินไม่สมเหตุสมผลเช่นของชนิดเดียวกันไม่ควรต่างกันมากถ้าไม่มีโปรโมชั่น
แล้วต้องระวังเรื่องของขนาดเช่น ยี่ห้อเดียวกันแต่คนละขนาด และ แพ็คที่ต่างกัน อย่างเช่น น้ำสิงห์ ขนาด 600 มล แพ็ค 12 ควรเทียบ กับ น้ำสิงห์ ขนาด 600 มล แพ็ค 12 ถ้าไม่มีให้ข้าม หรือถ้ามีแต่เท่ากันก็ข้าม



โปรดเขียนรายงานวิเคราะห์เชิงกลยุทธ์เป็นภาษาไทย โดยครอบคลุม:

1. **ภาพรวมตำแหน่งราคา** — Lotus อยู่ในฐานะใดเมื่อเทียบ Big C โดยรวม
2. **จุดเปราะบาง** — สินค้าที่ Lotus แพงกว่าและมีความเสี่ยงสูญเสียลูกค้า พร้อมระบุ priority ว่าสินค้าใดควรแก้ก่อน (ต้องระบุราคาและ % ลดทั้งสองฝั่งทุกรายการ)
3. **จุดแข็ง** — สินค้าที่ Lotus ได้เปรียบ พร้อมราคาทั้งสองฝั่ง และวิธีใช้ประโยชน์จากจุดนี้ให้เกิดผลสูงสุด (ต้องระบุราคาและ % ลดทั้งสองฝั่งทุกรายการ)
4. **วิเคราะห์โปรโมชัน** — เปรียบเทียบ promotion strategy ของสองร้าน พร้อมตัวอย่างสินค้าที่มีโปรโมชันน่าสนใจ (ต้องระบุราคาและ % ลดทั้งสองฝั่งทุกรายการ)
5. **คำแนะนำเชิงกลยุทธ์** — 3 action items ที่ทำได้จริงในสัปดาห์นี้"""

    insight = call_groq(prompt)

    hook.run(
        "INSERT INTO ai_answer (insight_type, content, created_at) VALUES (%s, %s, NOW())",
        parameters=('pricing_analysis', insight),
    )
    logging.info(f"\n{'='*60}\nAI INSIGHTS:\n{insight}\n{'='*60}")


with DAG(
    dag_id='price_pipeline',
    default_args=default_args,
    schedule_interval='0 19 * * *',
    catchup=False,
    tags=['price', 'scraping', 'ai'],
) as dag:

    t0  = PythonOperator(task_id='create_tables',   python_callable=create_tables)
    t1a = PythonOperator(task_id='scrape_lotus',    python_callable=scrape_lotus)
    t1b = PythonOperator(task_id='scrape_bigc',     python_callable=scrape_bigc)
    t2a = PythonOperator(task_id='insert_lotus',    python_callable=insert_lotus)
    t2b = PythonOperator(task_id='insert_bigc',     python_callable=insert_bigc)
    t3  = PythonOperator(task_id='match_products',  python_callable=match_products)
    t4  = PythonOperator(task_id='analyze_pricing', python_callable=analyze_pricing)

    t0 >> [t1a, t1b]
    t1a >> t2a
    t1b >> t2b
    [t2a, t2b] >> t3 >> t4

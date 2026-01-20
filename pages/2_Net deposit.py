import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Financial Dashboard", layout="wide")

st.title("📊 ระบบสรุปยอดธุรกรรมและค่าคอมมิชชัน")

uploaded_file = st.file_uploader("อัปโหลดไฟล์ Parquet", type="parquet")

if uploaded_file:
    # 1. อ่านไฟล์และล้างชื่อคอลัมน์ (ตัดช่องว่าง + ตัวพิมพ์เล็ก)
    df = pd.read_parquet(uploaded_file)
    df.columns = [str(c).strip().lower() for c in df.columns]
    cols = list(df.columns)

    # 2. ตรวจสอบประเภทไฟล์อัตโนมัติ (หัวใจสำคัญเพื่อแก้ Error)
    if 'user id' in cols and 'amount' in cols:
        st.success("✅ ตรวจพบ: รายงานธุรกรรมรายวัน (Transaction Report)")
        # ใช้ " " ครอบ user id เพราะชื่อมีช่องว่าง
        query = """
        SELECT 
            "user id" AS ID,
            currency AS Currency,
            SUM(CASE WHEN lower(type) LIKE '%deposit%' THEN CAST(amount AS DOUBLE) ELSE 0 END) AS Deposit,
            SUM(CASE WHEN lower(type) LIKE '%withdraw%' THEN ABS(CAST(amount AS DOUBLE)) ELSE 0 END) AS Withdraw,
            0.0 AS Commission
        FROM df
        GROUP BY 1, 2
        """
    elif 'receiver_id' in cols:
        st.success("✅ ตรวจพบ: รายงานคอมมิชชัน (IB Report)")
        query = """
        SELECT 
            receiver_id AS ID,
            currency AS Currency,
            SUM(CAST(commission AS DOUBLE)) AS Commission,
            SUM(CAST(deposit AS DOUBLE)) AS Deposit,
            SUM(CAST(withdraw AS DOUBLE)) AS Withdraw
        FROM df
        GROUP BY 1, 2
        """
    else:
        st.error("❌ ไม่รองรับรูปแบบไฟล์นี้")
        st.info(f"คอลัมน์ที่ระบบตรวจพบในไฟล์ของคุณ: {cols}")
        st.stop()

    # 3. ประมวลผลและแยกยอดสรุป USC / USD (ตามที่คุณต้องการ)
    df_final = duckdb.query(query).df()
    df_final['Net_Deposit'] = df_final['Deposit'] - df_final['Withdraw']

    st.write("### 💰 ยอดรวมแยกตามสกุลเงิน")
    # วนลูปสร้างกล่อง Metric แยกตามสกุลเงินที่มีในไฟล์
    for curr in sorted(df_final['Currency'].unique()):
        df_curr = df_final[df_final['Currency'] == curr]
        with st.container():
            st.markdown(f"#### 💵 สกุลเงิน: {curr}")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("จำนวน ID", f"{len(df_curr):,}")
            c2.metric("Net Deposit รวม", f"{df_curr['Net_Deposit'].sum():,.2f}")
            c3.metric("ยอดฝาก", f"{df_curr['Deposit'].sum():,.2f}")
            c4.metric("ยอดถอน", f"{df_curr['Withdraw'].sum():,.2f}")
            st.write("---")

    # 4. กราฟและการแสดงผล
    tab1, tab2 = st.tabs(["📊 Top 20 Net Deposit", "📋 ตารางข้อมูล"])
    with tab1:
        top_20 = df_final.sort_values('Net_Deposit', ascending=False).head(20)
        fig = px.bar(top_20, x='ID', y='Net_Deposit', color='Currency', text_auto='.2s')
        st.plotly_chart(fig, use_container_width=True)
    with tab2:
        st.dataframe(df_final, use_container_width=True)

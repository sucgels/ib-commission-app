import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Universal Analysis Dashboard", layout="wide")

st.title("📊 ระบบวิเคราะห์ข้อมูล (รองรับไฟล์หลายรูปแบบ)")

uploaded_file = st.file_uploader("อัปโหลดไฟล์ Parquet (หรือไฟล์ที่แปลงมาจาก Excel ธุรกรรม)", type="parquet")

if uploaded_file:
    df = pd.read_parquet(uploaded_file)
    # ปรับหัวตารางให้เป็นพิมพ์เล็กและตัดช่องว่างเพื่อความแม่นยำ
    df.columns = [c.strip().lower() for c in df.columns]
    cols = df.columns

    # --- ตรวจสอบรูปแบบไฟล์ (Auto-Detection) ---
    # แบบที่ 1: ไฟล์ธุรกรรมใหม่ (มี User ID, Type, Amount)
    if 'user id' in cols and 'amount' in cols and 'type' in cols:
        st.info("💡 ตรวจพบ: ไฟล์รายงานธุรกรรมรายวัน (Transaction Report)")
        query = """
        SELECT 
            "user id" AS ID,
            currency AS Currency,
            SUM(CASE WHEN LOWER(type) LIKE '%deposit%' THEN CAST(amount AS DOUBLE) ELSE 0 END) AS Deposit,
            SUM(CASE WHEN LOWER(type) LIKE '%withdraw%' THEN ABS(CAST(amount AS DOUBLE)) ELSE 0 END) AS Withdraw,
            0.0 AS Commission -- ไฟล์นี้ไม่มีคอมมิชชัน
        FROM df
        GROUP BY 1, 2
        """
    # แบบที่ 2: ไฟล์ Commission เดิม (มี receiver_id, deposit, withdraw)
    elif 'receiver_id' in cols:
        st.info("💡 ตรวจพบ: ไฟล์สรุปคอมมิชชัน (IB Commission Report)")
        has_fin = 'deposit' in cols and 'withdraw' in cols
        query = f"""
        SELECT 
            receiver_id AS ID,
            currency AS Currency,
            SUM(CAST(commission AS DOUBLE)) AS Commission,
            {"SUM(CAST(deposit AS DOUBLE))" if has_fin else "0.0"} AS Deposit,
            {"SUM(CAST(withdraw AS DOUBLE))" if has_fin else "0.0"} AS Withdraw
        FROM df
        GROUP BY 1, 2
        """
    else:
        st.error("❌ รูปแบบไฟล์ไม่รองรับ หรือคอลัมน์ไม่ครบ")
        st.stop()

    # --- ประมวลผลและแสดงผลแยกตาม Currency ---
    df_raw = duckdb.query(query).df()
    df_raw['Net_Deposit'] = df_raw['Deposit'] - df_raw['Withdraw']
    
    # สรุปยอดแยกตาม Currency (USD, USC)
    st.write("### 💰 สรุปยอดรวมแยกตามสกุลเงิน")
    currencies = df_raw['Currency'].unique()
    
    for curr in currencies:
        df_curr = df_raw[df_raw['Currency'] == curr]
        with st.container():
            st.subheader(f"💵 สกุลเงิน: {curr}")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("จำนวน ID", f"{len(df_curr):,}")
            c2.metric("Commission", f"{df_curr['Commission'].sum():,.2f}")
            c3.metric("Net Deposit", f"{df_curr['Net_Deposit'].sum():,.2f}")
            c4.metric("ฝาก/ถอน", f"{df_curr['Deposit'].sum():,.0f} / {df_curr['Withdraw'].sum():,.0f}")
            st.write("---")

    # --- กราฟแท่งเปรียบเทียบ ---
    st.write(f"### 📊 อันดับ Net Deposit สูงสุด (Top 20)")
    # กรองเฉพาะค่า Net_Deposit > 0 มาโชว์ในกราฟ
    df_plot = df_raw[df_raw['Net_Deposit'] > 0].sort_values('Net_Deposit', ascending=False).head(20)
    if not df_plot.empty:
        fig = px.bar(df_plot, x='ID', y='Net_Deposit', color='Currency', text_auto='.2s')
        st.plotly_chart(fig, use_container_width=True)

    # --- ตารางข้อมูล ---
    st.write("### 📋 ข้อมูลดิบที่คำนวณได้")
    st.dataframe(df_raw, use_container_width=True)

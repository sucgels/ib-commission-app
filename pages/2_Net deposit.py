import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Financial Dashboard", layout="wide")
st.title("📊 ระบบสรุปยอดอัจฉริยะ (V8.0)")

uploaded_file = st.file_uploader("อัปโหลดไฟล์ Parquet", type="parquet")

if uploaded_file:
    df = pd.read_parquet(uploaded_file)
    # ล้างชื่อคอลัมน์ให้เหลือแต่ตัวอักษรเล็กและไม่มีช่องว่าง
    df.columns = [str(c).strip().lower() for c in df.columns]
    cols = list(df.columns)

    # ฟังก์ชันช่วยหาคอลัมน์ที่ใกล้เคียงที่สุด
    def find_col(targets, current_cols):
        for t in targets:
            for c in current_cols:
                if t in c: return c
        return None

    # ค้นหาคอลัมน์สำคัญ
    col_id = find_col(['user id', 'receiver_id', 'id'], cols)
    col_type = find_col(['type'], cols)
    col_amount = find_col(['amount'], cols)
    col_curr = find_col(['currency'], cols)
    col_comm = find_col(['commission'], cols)

    if col_id and col_amount:
        st.success(f"✅ ตรวจพบข้อมูล: ID({col_id}), Amount({col_amount})")
        
        # จัดการชื่อคอลัมน์ให้ SQL อ่านง่าย
        df_temp = df.copy()
        df_temp = df_temp.rename(columns={col_id: 'target_id', col_amount: 'target_amt', col_curr: 'target_curr'})
        if col_type: df_temp = df_temp.rename(columns={col_type: 'target_type'})
        if col_comm: df_temp = df_temp.rename(columns={col_comm: 'target_comm'})

        # เขียน SQL แบบยืดหยุ่น
        query = """
        SELECT 
            target_id AS ID,
            target_curr AS Currency,
            SUM(CASE WHEN lower(CAST(target_type AS VARCHAR)) LIKE '%deposit%' THEN CAST(target_amt AS DOUBLE) ELSE 0 END) AS Deposit,
            SUM(CASE WHEN lower(CAST(target_type AS VARCHAR)) LIKE '%withdraw%' THEN ABS(CAST(target_amt AS DOUBLE)) ELSE 0 END) AS Withdraw,
            SUM(CASE WHEN 'target_comm' IN (SELECT column_name FROM (SELECT * FROM df_temp LIMIT 0)) THEN CAST(target_comm AS DOUBLE) ELSE 0 END) AS Commission
        FROM df_temp
        GROUP BY 1, 2
        """
        
        df_final = duckdb.query(query).df()
        df_final['Net_Deposit'] = df_final['Deposit'] - df_final['Withdraw']

        # --- แสดง Metrics แยก USD/USC ---
        st.write("### 💰 ยอดรวมแยกตามสกุลเงิน")
        for curr in sorted(df_final['Currency'].unique()):
            df_curr = df_final[df_final['Currency'] == curr]
            with st.container():
                st.markdown(f"#### 💵 สกุลเงิน: {curr}")
                c1, c2, c3 = st.columns(3)
                c1.metric("จำนวน ID", f"{len(df_curr):,}")
                c2.metric("Net Deposit รวม", f"{df_curr['Net_Deposit'].sum():,.2f}")
                c3.metric("ฝาก / ถอน", f"{df_curr['Deposit'].sum():,.0f} / {df_curr['Withdraw'].sum():,.0f}")
                st.write("---")

        # กราฟแท่ง
        st.write("### 📊 Top 20 Net Deposit")
        fig = px.bar(df_final.sort_values('Net_Deposit', ascending=False).head(20), 
                     x='ID', y='Net_Deposit', color='Currency', text_auto='.2s')
        st.plotly_chart(fig, use_container_width=True)
        
        st.write("### 📋 ตารางข้อมูลสรุป")
        st.dataframe(df_final, use_container_width=True)
    else:
        st.error("❌ หาคอลัมน์ ID หรือ Amount ไม่เจอ")
        st.info(f"คอลัมน์ที่มีในไฟล์: {cols}")

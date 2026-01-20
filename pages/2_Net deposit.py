import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Universal Analysis", layout="wide")

st.title("📊 ระบบวิเคราะห์ข้อมูล (รองรับไฟล์ธุรกรรมใหม่)")

uploaded_file = st.file_uploader("อัปโหลดไฟล์ Parquet", type="parquet")

if uploaded_file:
    df = pd.read_parquet(uploaded_file)
    # ทำความสะอาดชื่อคอลัมน์: ตัดช่องว่างหน้าหลัง และทำให้เป็นพิมพ์เล็ก
    df.columns = [str(c).strip().lower() for c in df.columns]
    cols = list(df.columns)

    # --- เช็คคอลัมน์ที่มีอยู่ในไฟล์จริง (เพื่อแก้ Error สีแดงของคุณ) ---
    is_transaction_file = 'user id' in cols and 'amount' in cols
    is_commission_file = 'receiver_id' in cols

    if is_transaction_file:
        st.success("✅ ตรวจพบ: รายงานธุรกรรม (Transaction Report)")
        # ใช้คำสั่ง SQL เพื่อแยก Deposit และ Withdrawal จากคอลัมน์ Type
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
    elif is_commission_file:
        st.success("✅ ตรวจพบ: รายงานคอมมิชชัน (Commission Report)")
        has_f = 'deposit' in cols and 'withdraw' in cols
        query = f"""
        SELECT 
            receiver_id AS ID,
            currency AS Currency,
            SUM(CAST(commission AS DOUBLE)) AS Commission,
            {"SUM(CAST(deposit AS DOUBLE))" if has_f else "0.0"} AS Deposit,
            {"SUM(CAST(withdraw AS DOUBLE))" if has_f else "0.0"} AS Withdraw
        FROM df
        GROUP BY 1, 2
        """
    else:
        st.error("❌ ไม่รองรับรูปแบบไฟล์นี้")
        st.info(f"คอลัมน์ที่ตรวจพบในไฟล์ของคุณคือ: {cols}")
        st.stop()

    # ประมวลผลข้อมูลหลัก
    df_final = duckdb.query(query).df()
    df_final['Net_Deposit'] = df_final['Deposit'] - df_final['Withdraw']

    # --- แสดงยอดสรุปแยก USC / USD ---
    st.write("### 💰 สรุปยอดรวมแยกตามสกุลเงิน")
    for curr in df_final['Currency'].unique():
        df_curr = df_final[df_final['Currency'] == curr]
        with st.expander(f"💵 สกุลเงิน: {curr}", expanded=True):
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("จำนวน ID", f"{len(df_curr):,}")
            c2.metric("Commission", f"{df_curr['Commission'].sum():,.2f}")
            c3.metric("Net Deposit", f"{df_curr['Net_Deposit'].sum():,.2f}")
            c4.metric("ยอดฝาก / ถอน", f"{df_curr['Deposit'].sum():,.0f} / {df_curr['Withdraw'].sum():,.0f}")

    # --- กราฟแสดงผล ---
    st.write("---")
    val_col = 'Net_Deposit' if df_final['Net_Deposit'].sum() != 0 else 'Commission'
    
    tab1, tab2 = st.tabs(["📊 Top 20 Bar Chart", "🌲 Treemap"])
    with tab1:
        fig_bar = px.bar(df_final.sort_values(val_col, ascending=False).head(20), 
                         x='ID', y=val_col, color='Currency', text_auto='.2s')
        st.plotly_chart(fig_bar, use_container_width=True)
    with tab2:
        fig_tree = px.treemap(df_final[df_final[val_col] > 0], 
                              path=['Currency', 'ID'], values=val_col, color=val_col)
        st.plotly_chart(fig_tree, use_container_width=True)

    st.write("### 📋 ตารางข้อมูลทั้งหมด")
    st.dataframe(df_final, use_container_width=True)

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Multi-Format Analysis", layout="wide")

st.title("📊 ระบบวิเคราะห์ข้อมูลธุรกรรมและค่าคอมมิชชัน")

uploaded_file = st.file_uploader("อัปโหลดไฟล์ Parquet (รองรับทั้งไฟล์ธุรกรรมและไฟล์สรุปคอม)", type="parquet")

if uploaded_file:
    df = pd.read_parquet(uploaded_file)
    # ล้างชื่อคอลัมน์ให้เป็นพิมพ์เล็กและไม่มีช่องว่างส่วนเกิน
    df.columns = [c.strip().lower() for c in df.columns]
    cols = df.columns

    # --- ส่วนตรวจสอบโครงสร้างไฟล์ ---
    # 1. กรณีเป็นไฟล์ 'customer_reports_transactions' (มีคอลัมน์ User ID, Type, Amount)
    if 'user id' in cols and 'type' in cols and 'amount' in cols:
        st.success("✅ ตรวจพบรูปแบบ: ไฟล์รายงานธุรกรรมรายวัน")
        query = """
        SELECT 
            "user id" AS ID,
            currency AS Currency,
            SUM(CASE WHEN LOWER(type) LIKE '%deposit%' THEN CAST(amount AS DOUBLE) ELSE 0 END) AS Deposit,
            SUM(CASE WHEN LOWER(type) LIKE '%withdrawal%' OR LOWER(type) LIKE '%withdraw%' 
                     THEN ABS(CAST(amount AS DOUBLE)) ELSE 0 END) AS Withdraw,
            0.0 AS Commission -- ไฟล์ประเภทนี้มักไม่มีคอมมิชชันในตัว
        FROM df
        GROUP BY 1, 2
        """
    # 2. กรณีเป็นไฟล์สรุป Commission เดิม (มี receiver_id)
    elif 'receiver_id' in cols:
        st.success("✅ ตรวจพบรูปแบบ: ไฟล์สรุปค่าคอมมิชชัน IB")
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
        st.error("❌ ไม่รองรับรูปแบบไฟล์นี้ กรุณาตรวจสอบหัวตาราง")
        st.stop()

    # ประมวลผลข้อมูล
    df_final = duckdb.query(query).df()
    df_final['Net_Deposit'] = df_final['Deposit'] - df_final['Withdraw']

    # --- ส่วนแสดงยอดรวมแยกตาม USC / USD ---
    st.write("### 💰 สรุปยอดรวมแยกตามสกุลเงิน")
    for curr in df_final['Currency'].unique():
        df_curr = df_final[df_final['Currency'] == curr]
        with st.expander(f"💵 สกุลเงิน: {curr} (คลิกเพื่อดูรายละเอียด)", expanded=True):
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("จำนวน ID", f"{len(df_curr):,}")
            c2.metric("Commission", f"{df_curr['Commission'].sum():,.2f}")
            c3.metric("Net Deposit", f"{df_curr['Net_Deposit'].sum():,.2f}")
            c4.metric("ยอดฝาก / ยอดถอน", f"{df_curr['Deposit'].sum():,.0f} / {df_curr['Withdraw'].sum():,.0f}")

    # --- ส่วนการแสดงกราฟ ---
    st.write("---")
    tab1, tab2 = st.tabs(["📊 กราฟแท่ง Top 20", "🌲 Treemap"])
    
    val_to_plot = 'Net_Deposit' if df_final['Net_Deposit'].sum() != 0 else 'Commission'
    
    with tab1:
        top_20 = df_final.sort_values(val_to_plot, ascending=False).head(20)
        fig_bar = px.bar(top_20, x='ID', y=val_to_plot, color='Currency', text_auto='.2s',
                         title=f"20 อันดับสูงสุดตาม {val_to_plot}")
        st.plotly_chart(fig_bar, use_container_width=True)
        
    with tab2:
        fig_tree = px.treemap(df_final[df_final[val_to_plot] > 0], 
                              path=['Currency', 'ID'], values=val_to_plot, color=val_to_plot,
                              color_continuous_scale='RdYlGn')
        st.plotly_chart(fig_tree, use_container_width=True)

    # ตารางรายละเอียด
    st.write("### 📋 ตารางข้อมูลสรุป")
    st.dataframe(df_final, use_container_width=True)

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Multi-Analysis Dashboard", layout="wide")

st.title("📊 ระบบวิเคราะห์ข้อมูลอัจฉริยะ")

uploaded_file = st.file_uploader("อัปโหลดไฟล์ Parquet (รองรับทั้งแบบมีและไม่มียอดฝาก-ถอน)", type="parquet")

if uploaded_file:
    df = pd.read_parquet(uploaded_file)
    cols = [c.lower() for c in df.columns]
    
    # ตรวจสอบสถานะข้อมูล
    has_finance = 'deposit' in cols and 'withdraw' in cols
    has_commission = 'commission' in cols

    # --- ส่วนการประมวลผลข้อมูล ---
    if has_finance:
        # กรณีมีข้อมูลการเงินครบ
        query = """
        SELECT 
            receiver_id AS ID,
            currency AS Currency,
            SUM(CAST(commission AS DOUBLE)) AS Total_Commission,
            SUM(CAST(deposit AS DOUBLE)) AS Total_Deposit,
            SUM(CAST(withdraw AS DOUBLE)) AS Total_Withdraw,
            (SUM(CAST(deposit AS DOUBLE)) - SUM(CAST(withdraw AS DOUBLE))) AS Net_Deposit
        FROM df
        GROUP BY 1, 2
        """
        title_text = "Net Deposit Distribution"
        value_col = "Net_Deposit"
        color_scale = 'RdYlGn' # เขียว-เหลือง-แดง
    else:
        # กรณีมีแค่ Commission
        query = """
        SELECT 
            receiver_id AS ID,
            currency AS Currency,
            SUM(CAST(commission AS DOUBLE)) AS Total_Commission
        FROM df
        GROUP BY 1, 2
        """
        title_text = "Commission Distribution (No Deposit Data)"
        value_col = "Total_Commission"
        color_scale = 'Blues' # สีฟ้า

    df_final = duckdb.query(query).df()

    # --- แสดงผลหน้าเว็บ ---
    st.write(f"### 🌲 {title_text}")
    
    # วาด Treemap (กรองค่าที่มากกว่า 0 เพื่อไม่ให้กราฟ Error)
    df_tree = df_final[df_final[value_col] > 0]
    
    if not df_tree.empty:
        fig = px.treemap(
            df_tree, 
            path=['Currency', 'ID'], 
            values=value_col,
            color=value_col,
            color_continuous_scale=color_scale,
            title=f"วิเคราะห์ตามยอด {value_col}"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("⚠️ ข้อมูลในคอลัมน์หลักเป็น 0 หรือติดลบ ไม่สามารถวาด Treemap ได้")

    # --- ตารางข้อมูลสรุป ---
    st.write("### 📋 ตารางสรุปข้อมูลทั้งหมด")
    st.dataframe(df_final.style.format(precision=2), use_container_width=True)

    # ปุ่มขยายดูโครงสร้างไฟล์
    with st.expander("🔍 ดูข้อมูลดิบและคอลัมน์ที่ตรวจพบ"):
        st.write("Columns found:", list(df.columns))
        st.write(df.head(5))

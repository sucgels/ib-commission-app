import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Universal Data Analysis", layout="wide")

st.title("📊 ระบบวิเคราะห์ข้อมูลธุรกรรมและคอมมิชชัน")

uploaded_file = st.file_uploader("อัปโหลดไฟล์ Parquet", type="parquet")

if uploaded_file:
    # อ่านไฟล์และล้างชื่อคอลัมน์ให้สะอาด
    df = pd.read_parquet(uploaded_file)
    df.columns = [str(c).strip().lower() for c in df.columns]
    cols = list(df.columns)

    # --- ตรวจจับรูปแบบไฟล์อัตโนมัติ ---
    # ตรวจเช็คว่ามีคอลัมน์สำคัญของไฟล์ธุรกรรมไหม (User ID, Amount, Type)
    is_transaction = 'user id' in cols and 'amount' in cols
    is_commission = 'receiver_id' in cols

    if is_transaction:
        st.success("✅ ตรวจพบ: รายงานธุรกรรม (Transaction Report)")
        # ใช้คำสั่ง SQL แยก Deposit และ Withdrawal จากคอลัมน์ Type
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
    elif is_commission:
        st.success("✅ ตรวจพบ: รายงานคอมมิชชัน (IB Commission)")
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
        st.info(f"คอลัมน์ที่ระบบตรวจพบ: {cols}")
        st.stop()

    # ประมวลผลข้อมูลหลัก
    df_final = duckdb.query(query).df()
    df_final['Net_Deposit'] = df_final['Deposit'] - df_final['Withdraw']

    # --- แสดงยอดสรุปแยก USC / USD ---
    st.write("### 💰 สรุปยอดรวมแยกตามสกุลเงิน")
    for curr in df_final['Currency'].unique():
        df_curr = df_final[df_final['Currency'] == curr]
        with st.container():
            st.markdown(f"#### 💵 สกุลเงิน: {curr}")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("จำนวน ID", f"{len(df_curr):,}")
            c2.metric("Commission", f"{df_curr['Commission'].sum():,.2f}")
            c3.metric("Net Deposit", f"{df_curr['Net_Deposit'].sum():,.2f}")
            c4.metric("ฝาก / ถอน", f"{df_curr['Deposit'].sum():,.0f} / {df_curr['Withdraw'].sum():,.0f}")
            st.write("---")

    # --- ส่วนการแสดงกราฟ ---
    val_col = 'Net_Deposit' if df_final['Net_Deposit'].sum() != 0 else 'Commission'
    
    tab1, tab2 = st.tabs(["📊 Top 20 Bar Chart", "🌲 Treemap"])
    with tab1:
        top_20 = df_final.sort_values(val_col, ascending=False).head(20)
        fig_bar = px.bar(top_20, x='ID', y=val_col, color='Currency', text_auto='.2s',
                         title=f"20 อันดับสูงสุดตาม {val_col}")
        st.plotly_chart(fig_bar, use_container_width=True)
    with tab2:
        fig_tree = px.treemap(df_final[df_final[val_col] > 0], 
                              path=['Currency', 'ID'], values=val_col, color=val_col,
                              color_continuous_scale='RdYlGn')
        st.plotly_chart(fig_tree, use_container_width=True)

    st.write("### 📋 ตารางข้อมูลสรุป")
    st.dataframe(df_final, use_container_width=True)

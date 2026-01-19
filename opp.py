import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import os

st.set_page_config(page_title="IB Commission Multi-Summarizer (Pro)", layout="wide")

st.title("📊 ระบบสรุปยอด Commission (โหมดประมวลผลไฟล์ใหญ่)")
st.write("รองรับไฟล์ขนาดใหญ่สูงสุด 2GB ด้วยเทคโนโลยี DuckDB Direct Processing")

uploaded_files = st.file_uploader("เลือกไฟล์ CSV ของคุณ", type="csv", accept_multiple_files=True)

if uploaded_files:
    if st.button("เริ่มคำนวณยอดสรุป (โหมดประหยัด RAM)"):
        with st.spinner("กำลังประมวลผลข้อมูล... กรุณารอสักครู่ (ไม่ทำให้แอปค้าง)"):
            try:
                con = duckdb.connect()
                
                # วิธีใหม่: บันทึกไฟล์ลง Disk ชั่วคราวเพื่อให้ DuckDB อ่านโดยไม่กิน RAM
                temp_paths = []
                for f in uploaded_files:
                    path = f"temp_{f.name}"
                    with open(path, "wb") as buffer:
                        buffer.write(f.getbuffer())
                    temp_paths.append(path)
                
                # ใช้ DuckDB อ่านไฟล์จาก Disk ตรงๆ (เร็วและประหยัด RAM ที่สุด)
                query = f"""
                SELECT 
                    receiver_id AS ID,
                    ROUND(SUM(CASE WHEN currency = 'USC' THEN commission ELSE 0 END), 2) AS Total_USC,
                    ROUND(SUM(CASE WHEN currency = 'USD' THEN commission ELSE 0 END), 2) AS Total_USD,
                    COUNT(*) AS Total_Orders
                FROM read_csv_auto({temp_paths})
                GROUP BY receiver_id
                ORDER BY Total_USC DESC;
                """
                
                df_final = con.execute(query).df()
                
                # แสดงผล Metrics และกราฟ (เหมือนเดิม)
                st.success(f"✅ ประมวลผลสำเร็จ! พบข้อมูล {len(df_final)} คน")
                
                col1, col2, col3 = st.columns(3)
                col1.metric("จำนวน ID", f"{len(df_final):,.0f}")
                col2.metric("ยอดรวม USC", f"{df_final['Total_USC'].sum():,.2f}")
                col3.metric("ยอดรวม USD", f"{df_final['Total_USD'].sum():,.2f}")

                st.divider()
                st.subheader("ยอด Commission Top 10 ID (USC)")
                fig = px.bar(df_final.head(10), x='ID', y='Total_USC', color_discrete_sequence=['#00CC96'])
                st.plotly_chart(fig, use_container_width=True)

                st.dataframe(df_final, use_container_width=True)
                
                # ปุ่มดาวน์โหลด
                st.download_button("📥 โหลดไฟล์สรุป (CSV)", df_final.to_csv(index=False).encode('utf-8-sig'), "Summary.csv", "text/csv")
                
                # ลบไฟล์ชั่วคราวออกเพื่อคืนพื้นที่
                for p in temp_paths:
                    if os.path.exists(p): os.remove(p)
                    
            except Exception as e:
                st.error(f"เกิดข้อผิดพลาด: {e}")

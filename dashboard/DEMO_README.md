# NYC Taxi Analytics Dashboard - Demo Mode

## 🎭 Chạy Dashboard với Demo Data (Không cần BigQuery)

Vì dự án BigQuery đã bị disable billing, bạn có thể chạy dashboard với dữ liệu giả lập để demo.

### Cách 1: Sử dụng script run_demo.py (Khuyến nghị)

```bash
cd dashboard
streamlit run run_demo.py
```

### Cách 2: Set environment variable

**PowerShell:**
```powershell
cd dashboard
$env:DEMO_MODE="True"
streamlit run streamlit_dashboard.py
```

**Command Prompt:**
```cmd
cd dashboard
set DEMO_MODE=True
streamlit run streamlit_dashboard.py
```

**Linux/Mac:**
```bash
cd dashboard
export DEMO_MODE=True
streamlit run streamlit_dashboard.py
```

## ✨ Tính năng Demo Mode

Dashboard sẽ tự động sinh dữ liệu giả lập cho TẤT CẢ các tab:

### Tab 1: 🗺️ Fare Prediction
- Mock weather data (nhiệt độ, độ ẩm, gió)
- Fake high-demand zones trên bản đồ Manhattan
- Fare prediction dựa trên công thức đơn giản (không cần BQML model)

### Tab 2: 📊 Hourly Demand Heatmap  
- 50 zones giả lập với H3 IDs hợp lệ
- Demand data cho 24 giờ với peak hours realistic
- Color-coded circles theo mức độ demand

### Tab 3: 📈 Trip Analysis
- 500 trips giả lập (có thể tùy chỉnh số lượng)
- Fare vs Distance scatter plot
- Click để xem chi tiết từng trip

### Tab 4: 💎 Zone Analysis (RFM)
- 10 zones với RFM scores (Recency, Frequency, Monetary)
- Phân loại: Gold, Silver, Bronze, Watch, Dead segments
- Revenue contribution charts

### Tab 5: 🎯 PCA Clustering
- PCA demand scores cho các zones
- 4 clusters với geographic maps
- Demand visualization

### Tab 6: 🚖 Vendor Comparison
- Dữ liệu giả lập cho 2 vendors (Vendor 1 vs Vendor 2)
- **Hourly pattern**: 24 hours với rush hour peaks
- **Weekly pattern**: Cả 7 ngày trong tuần (đã fix missing days)
- **Monthly pattern**: Đầy đủ 12 tháng (đã fix - trước đó chỉ có 2 tháng)
- Speed comparison charts

## 🎯 Banner Thông Báo

Khi chạy demo mode, dashboard sẽ hiển thị banner màu xanh ở đầu:

```
🎭 DEMO MODE - Hiển thị dữ liệu giả lập (BigQuery đã tắt để tiết kiệm chi phí). 
Tất cả data và predictions đều là mock data để demo.
```

## 📊 Dữ Liệu Demo

Tất cả mock data được generate trong file `demo_data.py` với:
- **Realistic patterns**: Rush hours, weekend effects, seasonal trends
- **Random but plausible values**: Dựa trên statistics của NYC taxi thực tế
- **Reproducible**: Sử dụng np.random.seed(42) để consistent

## 🔄 Chuyển về Real Data Mode

Để chạy lại với BigQuery thật (khi enable billing):

```python
# Trong streamlit_dashboard.py, dòng 30:
DEMO_MODE = os.environ.get("DEMO_MODE", "False").lower() == "true"  # Đổi default "False"
```

Hoặc đơn giản là không set environment variable DEMO_MODE.

## 📝 Notes

- Demo mode KHÔNG cần credentials BigQuery
- Không cần file .env hay service account JSON
- Tất cả 6 tabs đều hoạt động đầy đủ
- Dữ liệu được cache để performance tốt
- Click vào map, charts vẫn interactive bình thường

## 🐛 Troubleshooting

**Lỗi import demo_data:**
```
ModuleNotFoundError: No module named 'demo_data'
```
→ Đảm bảo file `demo_data.py` nằm trong cùng thư mục với `streamlit_dashboard.py`

**Dashboard vẫn cố kết nối BigQuery:**
```
google.api_core.exceptions.Unauthorized: 401 Could not automatically determine credentials
```
→ Kiểm tra biến `DEMO_MODE` có được set đúng không:
```python
import os
print(os.environ.get("DEMO_MODE"))  # Should print "True"
```

## 💡 Tips cho Demo

1. **Tab 3 (Trip Analysis)**: Giảm số lượng trips xuống 100-200 để load nhanh hơn
2. **Tab 5 (PCA)**: Có thể skip nếu không cần thiết (yêu cầu scikit-learn)
3. **Resize browser**: Dashboard responsive, test trên màn hình nhỏ cũng OK

---

Chúc bạn demo thành công! 🚀

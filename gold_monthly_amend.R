# ==============================================================================
# R Script: Final Data Patching (Gold History Injection)
# ==============================================================================
library(data.table)
library(arrow)
library(zoo)     # 用于插值
library(readr)

# 1. 读取 Final Data (宽表)
# 假设你上一轮生成的宽表在这里
path_prices <- "data/final/asset_prices.parquet"
dt_prices <- read_parquet(path_prices)
setDT(dt_prices)

message(">>> 开始修补 GLD 历史数据...")
message(paste("    当前 GLD 起始日期:", min(dt_prices[!is.na(GLD), date])))

# 2. 读取并处理历史黄金数据 (Monthly CSV)
gold_csv <- fread("gold_monthly_hist_price.csv") # 请确保文件名正确
# 清洗日期 (你的CSV格式似乎是 MM/DD/YYYY)
gold_csv[, Date := as.Date(Date, format = "%m/%d/%Y")]
gold_csv[, Value := as.numeric(Value)]
setorder(gold_csv, Date)

# 3. 升频：月度 -> 日度 (插值)
# 创建一个从 1990年开始的每日时间序列
daily_dates <- seq(from = as.Date("1990-01-01"), to = max(gold_csv$Date), by = "day")
dt_gold_daily <- data.table(Date = daily_dates)

# 合并并插值
dt_gold_daily <- merge(dt_gold_daily, gold_csv, by = "Date", all.x = TRUE)
# 使用 zoo::na.approx 进行线性插值 (填补中间的空缺)
# rule=2 表示如果首尾缺失，保持最近的值
dt_gold_daily[, Price_Proxy := na.approx(Value, na.rm = FALSE, rule = 2)]

# 4. 执行拼接 (Splicing)
target_ticker <- "GLD"

# A. 找到 GLD ETF 的真实开始日期和价格
start_date_gld <- min(dt_prices[!is.na(get(target_ticker)), date])
price_gld_start <- dt_prices[date == start_date_gld, get(target_ticker)]

# B. 找到同一天(或最近)的历史代理价格
# 使用 rolling join 找到最近的一天
price_proxy_start <- dt_gold_daily[Date == start_date_gld, Price_Proxy]
if(length(price_proxy_start) == 0) {
  # 如果那是周末，找最近的前一天
  price_proxy_start <- dt_gold_daily[Date <= start_date_gld][.N, Price_Proxy]
}

# C. 计算拼接比率 (Ratio)
ratio <- price_gld_start / price_proxy_start
message(paste("    拼接比率:", round(ratio, 4), "(用于对齐历史价格)"))

# D. 生成调整后的历史价格序列
dt_gold_daily[, Price_Adj := Price_Proxy * ratio]

# 5. 填补数据 (Patching)
# 将调整后的历史价格合并回主表
dt_patch <- dt_gold_daily[Date < start_date_gld, .(date = Date, GLD_Hist = Price_Adj)]

# 合并
dt_prices <- merge(dt_prices, dt_patch, by = "date", all.x = TRUE)

# 核心动作：如果 GLD 是 NA，就用 GLD_Hist 填充
dt_prices[is.na(GLD), GLD := GLD_Hist]
# 删除临时列
dt_prices[, GLD_Hist := NULL]

message(paste("✅ 修补完成！GLD 新起始日期:", min(dt_prices[!is.na(GLD), date])))

# 6. 重新计算收益率表 (Sync Returns)
# 价格变了，收益率必须重算
message(">>> 正在重新生成收益率矩阵...")
dt_returns_patched <- copy(dt_prices)

# 对除了 date 以外的所有列计算收益率
cols_to_calc <- setdiff(names(dt_prices), "date")
dt_returns_patched[, (cols_to_calc) := lapply(.SD, function(x) x / shift(x) - 1), .SDcols = cols_to_calc]
dt_returns_patched[is.na(dt_returns_patched)] <- 0 # 第一天填0

# 7. 保存最终版 (Final Final)
write_parquet(dt_prices, "data/final/asset_prices_patched.parquet")
write_csv(dt_prices, "data/final/asset_prices_patched.csv")

write_parquet(dt_returns_patched, "data/final/asset_returns_patched.parquet")
write_csv(dt_returns_patched, "data/final/asset_returns_patched.csv")

message("🎉 最终文件已保存为 *_patched.parquet/csv")
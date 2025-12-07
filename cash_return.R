# ==============================================================================
# R Script: 生成 Cash 资产 (SHV Proxy) - 原生 API 版 (No Quantmod)
# ==============================================================================

# 1. 检查并加载必要的包
packages <- c("httr", "jsonlite", "data.table", "arrow", "zoo")
new_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
if(length(new_packages)) install.packages(new_packages)

library(httr)
library(jsonlite)
library(data.table)
library(arrow)
library(zoo) # 专门用于 na.locf (Last Observation Carried Forward)

# 2. 配置参数
API_KEY <- "fec35fc025bec5dc712a7cb2e2eb457d"
SERIES_ID <- "DGS3MO" # 3-Month Treasury Constant Maturity Rate
START_DATE <- "1990-01-01"

# 创建保存目录
dir.create("data/processed", recursive = TRUE, showWarnings = FALSE)

message(">>> 正在通过原生 API 连接 FRED 下载 DGS3MO...")

# 3. 发送 HTTP GET 请求
# 这是 FRED API 的标准 Endpoint
url <- "https://api.stlouisfed.org/fred/series/observations"

response <- GET(url, query = list(
  series_id = SERIES_ID,
  api_key = API_KEY,
  file_type = "json",
  observation_start = START_DATE
))

# 4. 检查请求状态
if (status_code(response) != 200) {
  stop(paste("API 请求失败! 状态码:", status_code(response)))
} else {
  message("    API 连接成功，正在解析数据...")
}

# 5. 解析 JSON 数据
content_text <- content(response, "text", encoding = "UTF-8")
json_data <- fromJSON(content_text)
dt_raw <- as.data.table(json_data$observations)

# 6. 数据清洗
# FRED 返回的 value 是字符型，且缺失值是用 "." 表示的
dt_clean <- dt_raw[, .(date, value)]
dt_clean[, date := as.Date(date)]
dt_clean[, value := as.numeric(value)] # "." 会自动变成 NA，并产生警告(可忽略)

# 排序
setorder(dt_clean, date)

# 7. 填补缺失值 (关键步骤)
# 债券市场节假日会导致 NA，必须用前值填充 (Locf)
# rule=2 表示如果开头就是 NA，保持 NA；如果结尾是 NA，用前值填
dt_clean[, value := na.locf(value, na.rm = FALSE, rule = 2)]

# 去除开头仍为 NA 的行（如果有）
dt_clean <- dt_clean[!is.na(value)]

# 8. 核心逻辑：年化利率 -> 净值曲线
# DGS3MO 是年化百分比 (e.g. 5.25 代表 5.25%)
yearly_rate <- dt_clean$value

# 计算日收益率 (假设一年 252 个交易日)
# 公式: r_daily = (1 + r_yearly/100)^(1/252) - 1
daily_ret <- (1 + yearly_rate/100)^(1/252) - 1

# 处理可能出现的异常值
daily_ret[is.na(daily_ret)] <- 0

# 构建净值曲线 (Base 100)
synth_price <- 100 * cumprod(1 + daily_ret)

# 9. 格式化输出 (Tidy Format)
dt_out <- data.table(
  date = dt_clean$date,
  ticker = "SGOV",
  price = synth_price,
  return = daily_ret,
  asset_name = "Cash_Proxy",
  source_type = "FRED_API_DGS3MO"
)

# 10. 保存 Parquet
# Save Price
write_parquet(dt_out[, .(date, ticker, price)], "data/processed/cash_prices.parquet")

# Save Return
write_parquet(dt_out[, .(date, ticker, return)], "data/processed/cash_returns.parquet")

message("✅ Cash (SGOV) 资产生成完毕！")
message(paste("    数据范围:", min(dt_out$date), "至", max(dt_out$date)))
message("    文件已保存至 data/processed/cash_prices.parquet & returns.parquet")

# 预览
print(tail(dt_out[, .(date, price, return)]))
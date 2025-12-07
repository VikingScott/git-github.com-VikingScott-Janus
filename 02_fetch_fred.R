# ==============================================================================
# 0. 环境设置与包加载
# ==============================================================================
# 如果没有安装 fredr，请取消下面注释运行
# install.packages("fredr")

library(fredr)
library(tidyverse)
library(lubridate)
library(fs)

process_dir <- "data/process"
if (!dir_exists(process_dir)) dir_create(process_dir)

# ==============================================================================
# 1. 配置 API KEY (关键步骤)
# ==============================================================================
# ⚠️⚠️⚠️ 请在引号内填入你的 FRED API Key ⚠️⚠️⚠️
my_api_key <- "fec35fc025bec5dc712a7cb2e2eb457d" 

fredr_set_key(my_api_key)

# ==============================================================================
# 2. 定义目标清单
# ==============================================================================
# 格式: FRED代码 = 我们想用的变量名
tickers_map <- c(
  "GDPNOW"      = "Fred_GDP_Now",
  "T10Y2Y"      = "Yield_10Y2Y",
  "VIXCLS"      = "VIX",
  "BAMLC0A0CM"  = "Credit_Spread"
)

# ==============================================================================
# 3. 定义下载函数
# ==============================================================================
fetch_series_via_api <- function(series_id, var_name) {
  message(paste("正在通过 API 下载:", series_id, "..."))
  
  tryCatch({
    # 使用 fredr 直接请求
    df <- fredr(
      series_id = series_id,
      observation_start = as.Date("1990-01-01")
    )
    
    # 检查数据
    if (nrow(df) == 0) return(NULL)
    
    # 清洗：只保留日期和数值，并改名
    df_clean <- df %>%
      select(Date = date, value) %>%
      rename(!!var_name := value)
    
    return(df_clean)
    
  }, error = function(e) {
    message(paste("❌ API 请求失败:", series_id, "\n原因:", e$message))
    return(NULL)
  })
}

# ==============================================================================
# 4. 执行下载与合并
# ==============================================================================

# 4.1 循环下载
# 使用 imap 遍历 named vector
list_of_dfs <- imap(tickers_map, ~fetch_series_via_api(.y, .x))

# 4.2 移除失败项
valid_dfs <- list_of_dfs %>% keep(~ !is.null(.))

if (length(valid_dfs) == 0) {
  stop("❌ 所有数据下载失败。请检查：1. API Key 是否正确; 2. 是否需要全局代理(VPN)。")
}

# 4.3 合并数据
message("下载完成，正在合并...")
df_merged <- valid_dfs %>%
  reduce(full_join, by = "Date") %>%
  arrange(Date)

# ==============================================================================
# 5. 日频化处理 (Forward Fill)
# ==============================================================================
# 生成标准日历骨架
date_grid <- tibble(
  Date = seq(from = min(df_merged$Date), to = Sys.Date(), by = "day")
)

df_fred_clean <- date_grid %>%
  left_join(df_merged, by = "Date") %>%
  # 核心：向前填充 (填补周末和 GDPNow 的空档)
  fill(everything(), .direction = "down") %>%
  filter(Date >= "1990-01-01")

# ==============================================================================
# 6. 保存
# ==============================================================================
output_path <- path(process_dir, "fred_daily_cleaned.csv")
write_csv(df_fred_clean, output_path)

message("------------------------------------------------")
message("✅ FRED API 数据处理完成！")
message(paste("输出路径:", output_path))
print(tail(df_fred_clean))
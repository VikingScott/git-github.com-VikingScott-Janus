# ==============================================================================
# 0. 准备工作
# ==============================================================================
if (!require("quantmod")) install.packages("quantmod")
if (!require("data.table")) install.packages("data.table")
if (!require("arrow")) install.packages("arrow") # 用于读写 Parquet

library(quantmod)
library(data.table)
library(arrow)

# 创建数据保存目录
dir.create("data/processed", recursive = TRUE, showWarnings = FALSE)

START_DATE <- "1990-01-01"

# ==============================================================================
# 1. 核心拼接函数 (保持不变，增加一点错误处理日志)
# ==============================================================================
splice_assets <- function(etf_ticker, proxy_ticker, start_date) {
  
  message(paste(">>> 正在处理:", etf_ticker, "(ETF) +", proxy_ticker, "(Proxy)"))
  
  # 下载数据
  tryCatch({
    dt_etf <- getSymbols(etf_ticker, src = "yahoo", from = start_date, auto.assign = FALSE)
    dt_proxy <- getSymbols(proxy_ticker, src = "yahoo", from = start_date, auto.assign = FALSE)
  }, error = function(e) {
    stop(paste("下载失败: ", e$message))
  })
  
  # 提取复权收盘价并计算收益率
  ret_etf <- dailyReturn(Ad(dt_etf))
  ret_proxy <- dailyReturn(Ad(dt_proxy))
  
  # 合并
  merged_xts <- merge(ret_etf, ret_proxy)
  names(merged_xts) <- c("ETF", "Proxy")
  
  # 拼接逻辑：ETF有数据用ETF，没有用Proxy
  merged_xts$Spliced_Ret <- ifelse(is.na(merged_xts$ETF), merged_xts$Proxy, merged_xts$ETF)
  merged_xts$Spliced_Ret[is.na(merged_xts$Spliced_Ret)] <- 0
  
  # 重建净值曲线 (Base 100)
  merged_xts$Synth_Price <- 100 * cumprod(1 + merged_xts$Spliced_Ret)
  
  # 转为 data.table
  dt_out <- data.table(date = index(merged_xts), coredata(merged_xts))
  
  # 清理字段，只保留我们需要的标准化字段
  dt_out <- dt_out[, .(date, 
                       ticker = etf_ticker, 
                       price = Synth_Price, 
                       return = Spliced_Ret,
                       source_type = ifelse(is.na(ETF), "Proxy", "Real"))]
  
  message(paste("    完成! 数据范围:", min(dt_out$date), "至", max(dt_out$date)))
  return(dt_out)
}

# ==============================================================================
# 2. 执行拼接 (修改了 EFA 的代理为 FDIVX)
# ==============================================================================

# A. 新兴市场 (EEM + VEIEX) - 涵盖 1997/1998 危机
dt_eem <- splice_assets("EEM", "VEIEX", START_DATE)

# B. 美股小盘 (IWM + NAESX) - 涵盖 2000 泡沫
dt_iwm <- splice_assets("IWM", "NAESX", START_DATE)

# C. [修复] 发达国家 (EFA + FDIVX)
# FDIVX (Fidelity Overseas) 始于 1984，完美覆盖 90 年代
dt_efa <- splice_assets("EFA", "FDIVX", START_DATE) 

# D. 美股大盘 (SPY) - SPY 1993年就有，前面缺的几年可以用 VFINX (Vanguard 500)
dt_spy <- splice_assets("SPY", "VFINX", START_DATE)

# 合并所有 Growth 资产
dt_all_growth <- rbind(dt_eem, dt_iwm, dt_efa, dt_spy)

# ==============================================================================
# 3. 拆分并保存 Parquet (Price 和 Return 分离)
# ==============================================================================

# ---- 生成 Price 表 (Long Format) ----
# 这种格式最适合数据库存储： Date | Ticker | Price
dt_price_out <- dt_all_growth[, .(date, ticker, price)]

# 保存
price_path <- "data/processed/growth_prices.parquet"
write_parquet(dt_price_out, price_path)
message(paste("✅ 价格数据已保存至:", price_path))


# ---- 生成 Return 表 (Long Format) ----
# 这种格式最适合计算： Date | Ticker | Return
dt_return_out <- dt_all_growth[, .(date, ticker, return)]

# 保存
return_path <- "data/processed/growth_returns.parquet"
write_parquet(dt_return_out, return_path)
message(paste("✅ 收益率数据已保存至:", return_path))

# ==============================================================================
# 4. (可选) 检查一下生成的数据结构
# ==============================================================================
print("--- Price Data Preview ---")
print(head(dt_price_out))

print("--- Return Data Preview ---")
print(head(dt_return_out))
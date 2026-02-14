import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import random
import time

from src.config import setup_env
from src.storage import get_db
from data_provider.base import DataFetcherManager

setup_env()


def random_sleep_before_call():
    sleep_time = random.uniform(0, 1)
    print(f"[防封禁] 随机休眠 {sleep_time:.2f} 秒")
    time.sleep(sleep_time)

if __name__ == "__main__":
    # print("### 上交所市场总览 ###")
    # random_sleep_before_call()
    # stock_sse_summary_df = ak.stock_sse_summary()
    # print(stock_sse_summary_df)
    #
    # print("### 深交所市场总览 ###")
    # yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    # random_sleep_before_call()
    # stock_szse_summary_df = ak.stock_szse_summary(date=yesterday)
    # print(stock_szse_summary_df)
    #
    # print("### 股票行业成交 ###")
    # random_sleep_before_call()
    # stock_szse_sector_summary_df = ak.stock_szse_sector_summary(symbol="当月", date="202601")
    # print(stock_szse_sector_summary_df)
    #
    # print("### 上交所日交易 ###")
    # random_sleep_before_call()
    # stock_sse_deal_daily_df = ak.stock_sse_deal_daily(date="20260213")
    # print(stock_sse_deal_daily_df)
    #
    # print("### 个股信息查询（东财） ###")
    # random_sleep_before_call()
    # stock_individual_info_em_df = ak.stock_individual_info_em(symbol="600418")
    # print(stock_individual_info_em_df)
    #
    # print("### 个股信息查询（雪球） ###")
    # random_sleep_before_call()
    # stock_individual_basic_info_xq_df = ak.stock_individual_basic_info_xq(symbol="SH600418")
    # print(stock_individual_basic_info_xq_df)

    # print("### 个股历史行情数据 ###")
    # random_sleep_before_call()
    # stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol="600418", period="daily", start_date="20260205", end_date='20260212',
    #                                         adjust="qfq")
    # print(stock_zh_a_hist_df)
    # stock_zh_a_hist_df = stock_zh_a_hist_df.sort_values("日期")
    # stock_zh_a_hist_df["MA5"] = stock_zh_a_hist_df["收盘"].rolling(5).mean()
    # print(stock_zh_a_hist_df[["日期", "收盘", "MA5"]].tail(5))

    # print("### 指数信息查询 ###")
    # random_sleep_before_call()
    # index_stock_info_df = ak.index_stock_info()
    # print(index_stock_info_df)

    # random_sleep_before_call()
    # print("### 指数成份股 ###")
    # index_stock_cons_df = ak.index_stock_cons(symbol="399432")
    # print(index_stock_cons_df)

    # random_sleep_before_call()
    # print("### 板块分类名称查询（东方财富） ###")
    # stock_board_industry_name_em_df = ak.stock_board_industry_name_em()
    # # Keep only: 排名, 板块名称, 板块代码, 总市值
    # cols = [c for c in ["排名", "板块名称", "板块代码", "总市值"] if c in stock_board_industry_name_em_df.columns]
    # df_to_save = stock_board_industry_name_em_df[cols].copy() if cols else stock_board_industry_name_em_df
    #
    # db = get_db()
    # n = db.save_industry_board_em(df_to_save)
    # print(f"已按总市值降序覆盖写入 industry_board_em 表，共 {n} 条")
    #
    # print("\n### 从数据库 SELECT 验证 ###")
    # rows = db.get_industry_board_em()
    # with pd.option_context("display.max_rows", None, "display.max_columns", None, "display.width", None):
    #     verify_df = pd.DataFrame([r.to_dict() for r in rows])
    #     print(verify_df)

    # 从板块表读出板块代码，遍历查询月 K 并计算本月 MA5
    # print("\n### 从板块表读板块代码，查月K并计算本月MA5 ###")
    # db = get_db()
    # rows = db.get_industry_board_em()
    # if not rows:
    #     print("板块表为空，请先执行上方“板块分类名称查询”并写入数据库。")
    # else:
    #     end_date = datetime.now()
    #     start_date = end_date - timedelta(days=6 * 31)  # 前 6 个月到本月
    #     start_str = start_date.strftime("%Y-%m-%d")
    #     end_str = end_date.strftime("%Y-%m-%d")
    #     manager = DataFetcherManager()
    #     fetcher = None
    #     for f in manager._fetchers:
    #         if hasattr(f, "get_industry_board_hist_em"):
    #             fetcher = f
    #             break
    #     if not fetcher:
    #         print("未找到支持 get_industry_board_hist_em 的数据源（如 Akshare）。")
    #     else:
    #         total = len(rows)
    #         result_list = []
    #         for i, row in enumerate(rows):
    #             random_sleep_before_call()
    #             print(f"[ {i + 1}/{total} ] 正在查询: {row.name} ({row.code})")
    #             df = fetcher.get_industry_board_hist_em(
    #                 symbol=row.code,
    #                 start_date=start_str,
    #                 end_date=end_str,
    #                 period="月k",
    #             )
    #             if df is None or len(df) < 5:
    #                 print(f"    -> 跳过（数据不足）")
    #                 continue
    #             df = df.sort_values("date").reset_index(drop=True)
    #             close_series = df["close"].astype(float)
    #             ma5 = close_series.rolling(window=5, min_periods=5).mean()
    #             if pd.isna(ma5.iloc[-1]):
    #                 print(f"    -> 跳过（MA5 无效）")
    #                 continue
    #             close_val = round(float(close_series.iloc[-1]), 2)
    #             ma5_val = round(float(ma5.iloc[-1]), 2)
    #             result_list.append({
    #                 "板块代码": row.code,
    #                 "板块名称": row.name,
    #                 "本月收盘": close_val,
    #                 "本月MA5": ma5_val,
    #             })
    #             print(f"    -> 本月收盘={close_val}, 本月MA5={ma5_val}")
    #         if result_list:
    #             out_df = pd.DataFrame(result_list)
    #             with pd.option_context("display.max_rows", None, "display.max_columns", None, "display.width", None):
    #                 print(out_df)
    #         else:
    #             print("未成功计算到任何板块的本月MA5。")

    # random_sleep_before_call()
    # print("### 板块分类详情查询（东方财富） ###")
    # stock_board_industry_spot_em_df = ak.stock_board_industry_spot_em(symbol="BK1211")
    # print(stock_board_industry_spot_em_df)

    # random_sleep_before_call()
    # print("### 板块成分股票列表查询（东方财富） ###")
    # stock_board_industry_cons_em_df = ak.stock_board_industry_cons_em(symbol="BK1211")
    # with pd.option_context("display.max_rows", None, "display.max_columns", None, "display.width", None):
    #     print(stock_board_industry_cons_em_df)

    # random_sleep_before_call()
    stock_rank_cxg_ths_df = ak.stock_rank_cxg_ths(symbol="历史新高")
    db = get_db()
    n = db.save_stock_rank_cxg_ths(stock_rank_cxg_ths_df)
    print(f"同花顺历史新高已入库，共 {n} 条")

    # 从库中查询创新高表全部数据并打印
    rows = db.get_stock_rank_cxg_ths()
    if rows:
        verify_df = pd.DataFrame([r.to_dict() for r in rows])
        with pd.option_context("display.max_rows", None, "display.max_columns", None, "display.width", None):
            print("\n### 创新高表全部数据（库中） ###")
            print(verify_df)
        # 连续两天及以上创新高的股票
        consecutive_2plus = [r for r in rows if (r.consecutive_days or 0) >= 2]
        if consecutive_2plus:
            df_2plus = pd.DataFrame([r.to_dict() for r in consecutive_2plus])
            with pd.option_context("display.max_rows", None, "display.max_columns", None, "display.width", None):
                print("\n### 连续两天及以上创新高（consecutive_days >= 2） ###")
                print(df_2plus)
        else:
            print("\n当前无连续两天及以上创新高的股票。")
    else:
        print("\n创新高表当前无数据。")
# -*- coding: utf-8 -*-
"""
===================================
股票数据接口
===================================

职责：
1. 提供 GET /api/v1/stocks/industries/above-ma5-monthly 五月均线上一级行业
2. 提供 GET /api/v1/stocks/{code}/quote 实时行情接口
3. 提供 GET /api/v1/stocks/{code}/history 历史行情接口
"""

import logging

from fastapi import APIRouter, HTTPException, Query

from api.v1.schemas.stocks import (
    StockQuote,
    StockHistoryResponse,
    KLineData,
    IndustryAboveMA5Item,
    IndustriesAboveMA5Response,
)
from api.v1.schemas.common import ErrorResponse
from src.services.stock_service import StockService
from src.market_analyzer import MarketAnalyzer

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/industries/above-ma5-monthly",
    response_model=IndustriesAboveMA5Response,
    responses={
        200: {"description": "五月均线上一级行业列表"},
        500: {"description": "服务器错误", "model": ErrorResponse},
    },
    summary="五月均线上一级行业",
    description="筛选当前月收盘价在五月均线（月线 MA5）之上的一级行业，按(收盘-均线)降序返回",
)
def get_industries_above_ma5_monthly(
    min_months: int = Query(6, ge=5, le=12, description="计算 MA5 所需最少月线数"),
    daily_days: int = Query(180, ge=90, le=365, description="拉取日线天数（用于重采样为月线）"),
) -> IndustriesAboveMA5Response:
    """
    获取在五月均线之上的一级行业列表。

    使用东财行业板块日线历史，重采样为月线，计算五月均线，筛选收盘价 > 五月均线的行业。
    依赖支持行业列表与行业历史的数据源（如 Akshare）。
    """
    try:
        analyzer = MarketAnalyzer()
        items = analyzer.get_industries_above_ma5_monthly(
            min_months=min_months,
            daily_days=daily_days,
        )
        industries = [
            IndustryAboveMA5Item(name=x["name"], close=x["close"], ma5_monthly=x["ma5_monthly"])
            for x in items
        ]
        return IndustriesAboveMA5Response(industries=industries)
    except Exception as e:
        logger.error(f"获取五月均线上一级行业失败: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "internal_error",
                "message": f"获取五月均线上一级行业失败: {str(e)}",
            },
        )


@router.get(
    "/{stock_code}/quote",
    response_model=StockQuote,
    responses={
        200: {"description": "行情数据"},
        404: {"description": "股票不存在", "model": ErrorResponse},
        500: {"description": "服务器错误", "model": ErrorResponse},
    },
    summary="获取股票实时行情",
    description="获取指定股票的最新行情数据"
)
def get_stock_quote(stock_code: str) -> StockQuote:
    """
    获取股票实时行情
    
    获取指定股票的最新行情数据
    
    Args:
        stock_code: 股票代码（如 600519、00700、AAPL）
        
    Returns:
        StockQuote: 实时行情数据
        
    Raises:
        HTTPException: 404 - 股票不存在
    """
    try:
        service = StockService()
        
        # 使用 def 而非 async def，FastAPI 自动在线程池中执行
        result = service.get_realtime_quote(stock_code)
        
        if result is None:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "not_found",
                    "message": f"未找到股票 {stock_code} 的行情数据"
                }
            )
        
        return StockQuote(
            stock_code=result.get("stock_code", stock_code),
            stock_name=result.get("stock_name"),
            current_price=result.get("current_price", 0.0),
            change=result.get("change"),
            change_percent=result.get("change_percent"),
            open=result.get("open"),
            high=result.get("high"),
            low=result.get("low"),
            prev_close=result.get("prev_close"),
            volume=result.get("volume"),
            amount=result.get("amount"),
            update_time=result.get("update_time")
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取实时行情失败: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "internal_error",
                "message": f"获取实时行情失败: {str(e)}"
            }
        )


@router.get(
    "/{stock_code}/history",
    response_model=StockHistoryResponse,
    responses={
        200: {"description": "历史行情数据"},
        422: {"description": "不支持的周期参数", "model": ErrorResponse},
        500: {"description": "服务器错误", "model": ErrorResponse},
    },
    summary="获取股票历史行情",
    description="获取指定股票的历史 K 线数据"
)
def get_stock_history(
    stock_code: str,
    period: str = Query("daily", description="K 线周期", pattern="^(daily|weekly|monthly)$"),
    days: int = Query(30, ge=1, le=365, description="获取天数")
) -> StockHistoryResponse:
    """
    获取股票历史行情
    
    获取指定股票的历史 K 线数据
    
    Args:
        stock_code: 股票代码
        period: K 线周期 (daily/weekly/monthly)
        days: 获取天数
        
    Returns:
        StockHistoryResponse: 历史行情数据
    """
    try:
        service = StockService()
        
        # 使用 def 而非 async def，FastAPI 自动在线程池中执行
        result = service.get_history_data(
            stock_code=stock_code,
            period=period,
            days=days
        )
        
        # 转换为响应模型
        data = [
            KLineData(
                date=item.get("date"),
                open=item.get("open"),
                high=item.get("high"),
                low=item.get("low"),
                close=item.get("close"),
                volume=item.get("volume"),
                amount=item.get("amount"),
                change_percent=item.get("change_percent")
            )
            for item in result.get("data", [])
        ]
        
        return StockHistoryResponse(
            stock_code=stock_code,
            stock_name=result.get("stock_name"),
            period=period,
            data=data
        )
    
    except ValueError as e:
        # period 参数不支持的错误（如 weekly/monthly）
        raise HTTPException(
            status_code=422,
            detail={
                "error": "unsupported_period",
                "message": str(e)
            }
        )
    except Exception as e:
        logger.error(f"获取历史行情失败: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "internal_error",
                "message": f"获取历史行情失败: {str(e)}"
            }
        )

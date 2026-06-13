"""Celery：批量预测与 Excel 导出（15 天发货预测 · 豆包方案）。"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
import tempfile

import pandas as pd

from app.intelligent_prediction.logging_utils import get_logger
from app.intelligent_prediction.db import get_prediction_session_factory
from app.intelligent_prediction.models import PredictionBatch
from app.intelligent_prediction.schemas.doubao_prediction import DoubaoBatchRequest
from app.intelligent_prediction.services.ai_client import get_ai_client
from app.intelligent_prediction.services.cache_manager import get_cache_manager
from app.intelligent_prediction.services.doubao_prediction_service import (
    DoubaoPredictionService,
    get_doubao_prediction_service,
)
from app.intelligent_prediction.services.doubao_prompt_builder import DoubaoPromptBuilder
from app.intelligent_prediction.services.audit_service import write_background_audit
from app.intelligent_prediction.tasks.celery_app import celery_app

logger = get_logger(__name__)


async def _run_batch_async(batch_id: str) -> None:
    SessionFactory = get_prediction_session_factory()
    async with SessionFactory() as session:
        batch = await session.get(PredictionBatch, batch_id)
        if batch is None:
            logger.error("prediction batch missing: %s", batch_id)
            return
        batch.status = "processing"
        batch.error_message = None
        await session.commit()
        await session.refresh(batch)
        try:
            meta = batch.meta or {}
            req = DoubaoBatchRequest.model_validate(meta)
            svc: DoubaoPredictionService = get_doubao_prediction_service(
                get_ai_client(), get_cache_manager(), DoubaoPromptBuilder()
            )
            results = await svc.predict_batch(req)
            await svc.persist_sync_results(session, results, batch_id=batch_id)
            rows: list[dict[str, object]] = []
            for pr in results:
                for it in pr.items:
                    rows.append(
                        {
                            "仓库": pr.warehouse,
                            "品类": pr.product_variety or "",
                            "目标日期": it.target_date.isoformat(),
                            "预测发货吨数": float(it.predicted_weight),
                            "发货概率": it.ship_probability,
                            "置信度": it.confidence_level,
                            "主要因素": it.main_factors,
                            "分析报告": pr.analysis_report[:500] if pr.analysis_report else "",
                        }
                    )
            df = pd.DataFrame(rows)
            tmp = Path(tempfile.gettempdir()) / f"prediction_{batch_id}.xlsx"
            df.to_excel(tmp, index=False, engine="openpyxl")
            batch.export_file_path = str(tmp)
            batch.status = "completed"
            batch.completed_at = datetime.now(timezone.utc)
            await session.commit()
        except Exception as e:
            logger.exception("prediction batch task failed batch_id=%s", batch_id)
            batch.status = "failed"
            batch.error_message = str(e)[:2000]
            batch.completed_at = datetime.now(timezone.utc)
            await session.commit()


async def _run_daily_prediction_async(batch_id: str) -> None:
    """每日AI预测：垂直/战略库房全量，普通合作库房仅近30天有发货量。"""
    SessionFactory = get_prediction_session_factory()
    async with SessionFactory() as session:
        batch = await session.get(PredictionBatch, batch_id)
        if batch is None:
            logger.error("daily prediction batch missing: %s", batch_id)
            return
        batch.status = "processing"
        batch.error_message = None
        await session.commit()

        try:
            from datetime import date as _date, timedelta
            from sqlalchemy import text, select as sa_select

            # 通过原始 SQL 从主库查询库房类型与近30天发货量（主库是 pymysql 同步连接）
            import pymysql
            from app.database import get_mysql_config

            mysql_cfg = get_mysql_config()
            conn = pymysql.connect(**mysql_cfg)
            warehouse_items: list[dict] = []
            try:
                with conn.cursor() as cur:
                    # 查询库房类型名称
                    cur.execute(
                        "SELECT wt.name FROM dict_warehouse_types wt "
                        "INNER JOIN dict_warehouses dw ON dw.warehouse_type_id = wt.id "
                        "GROUP BY wt.name"
                    )
                    type_rows = cur.fetchall()
                    type_names = {r[0] for r in type_rows}
                    logger.info("daily prediction: warehouse types found: %s", type_names)

                    # 垂直/战略类型关键词匹配
                    priority_type_names = {
                        t for t in type_names
                        if "垂直" in (t or "") or "战略" in (t or "")
                    }
                    logger.info(
                        "daily prediction: priority types (垂直/战略): %s", priority_type_names
                    )

                    # 查询全部活跃库房及其类型
                    cur.execute(
                        "SELECT dw.id, dw.name, COALESCE(wt.name, '') "
                        "FROM dict_warehouses dw "
                        "LEFT JOIN dict_warehouse_types wt ON dw.warehouse_type_id = wt.id "
                        "WHERE dw.is_active = 1"
                    )
                    all_warehouses = [
                        {"id": int(r[0]), "name": r[1], "type_name": r[2]}
                        for r in cur.fetchall()
                    ]

                    # 确定需要预测的库房
                    target_warehouses: set[str] = set()
                    for wh in all_warehouses:
                        if wh["type_name"] in priority_type_names:
                            target_warehouses.add(wh["name"])
                            logger.info(
                                "daily prediction: include priority warehouse %s (type=%s)",
                                wh["name"], wh["type_name"],
                            )

                    # 普通合作库房：仅近30天有发货量的
                    cutoff_date = (_date.today() - timedelta(days=30)).isoformat()
                    regular_wh_names = {
                        wh["name"] for wh in all_warehouses
                        if wh["type_name"] not in priority_type_names
                    }
                    if regular_wh_names:
                        placeholders = ",".join(["%s"] * len(regular_wh_names))
                        cur.execute(
                            f"SELECT DISTINCT warehouse FROM pd_ip_delivery_records "
                            f"WHERE warehouse IN ({placeholders}) AND delivery_date >= %s",
                            tuple(regular_wh_names) + (cutoff_date,),
                        )
                        active_regular = {r[0] for r in cur.fetchall()}
                        target_warehouses.update(active_regular)
                        logger.info(
                            "daily prediction: regular warehouses with recent deliveries: %s",
                            active_regular,
                        )
            finally:
                conn.close()

            if not target_warehouses:
                batch.status = "completed"
                batch.completed_at = datetime.now(timezone.utc)
                await session.commit()
                logger.info("daily prediction: no warehouses to process, batch_id=%s", batch_id)
                return

            logger.info(
                "daily prediction: total target warehouses=%s, batch_id=%s",
                len(target_warehouses), batch_id,
            )

            # 从 pd_ip_delivery_records 查询每个仓库的全部历史，按仓库级别预测
            wh_list = list(target_warehouses)
            from app.intelligent_prediction.models import DeliveryRecord, PredictionResult as PredictionResultRow
            from sqlalchemy import delete as sa_delete
            from app.intelligent_prediction.services.ai_client import get_ai_client
            from app.intelligent_prediction.services.cache_manager import get_cache_manager
            from app.intelligent_prediction.services.doubao_prediction_service import (
                DoubaoPredictionService,
                get_doubao_prediction_service,
            )
            from app.intelligent_prediction.services.doubao_prompt_builder import DoubaoPromptBuilder
            from app.intelligent_prediction.services.scheduled_prediction import (
                _load_smm_prices,
            )
            from app.intelligent_prediction.schemas.doubao_prediction import (
                DoubaoBatchRequest,
                DoubaoHistoryItem,
                DoubaoPredictionRequest,
            )

            smm_prices = await _load_smm_prices(session)

            items: list[DoubaoPredictionRequest] = []
            # 构建 history_map：仓库名 → 历史记录列表，用于 persist 时推断 regional_manager/smelter
            history_map: dict[str, list] = {}
            cutoff_history = _date.today() - timedelta(days=180)
            for wh_name in wh_list:
                stmt = (
                    sa_select(DeliveryRecord)
                    .where(DeliveryRecord.warehouse == wh_name)
                    .where(DeliveryRecord.delivery_date >= cutoff_history)
                    .order_by(DeliveryRecord.delivery_date)
                )
                res = await session.execute(stmt)
                records = list(res.scalars().all())

                history = []
                for record in records:
                    weather = record.import_weather
                    if not weather and record.weather_json:
                        weather = record.weather_json.get("text") or record.weather_json.get("description")
                    history.append(
                        DoubaoHistoryItem(
                            送货日期=record.delivery_date,
                            大区经理=record.regional_manager,
                            冶炼厂=record.smelter,
                            仓库=record.warehouse,
                            品类=record.product_variety,
                            天气=weather,
                            重量吨=record.weight,
                        )
                    )

                items.append(
                    DoubaoPredictionRequest(
                        warehouse=wh_name,
                        product_variety=None,
                        history=history,
                        smm_prices=smm_prices,
                        use_cache=True,
                    )
                )
                history_map[wh_name] = history

            if not items:
                batch.status = "completed"
                batch.completed_at = datetime.now(timezone.utc)
                await session.commit()
                logger.info("daily prediction: no items to predict, batch_id=%s", batch_id)
                return

            svc: DoubaoPredictionService = get_doubao_prediction_service(
                get_ai_client(), get_cache_manager(), DoubaoPromptBuilder()
            )
            from app.intelligent_prediction.services.predict_item_resolver import (
                resolve_one_predict_item,
            )

            results = []
            for req_item in items:
                result, _hist = await resolve_one_predict_item(session, svc, req_item)
                results.append(result)

            # 覆盖机制：先删除同类型旧批次结果，再写入新结果，保证缓存数据每日覆盖
            old_batch_stmt = sa_select(PredictionBatch.id).where(
                PredictionBatch.prediction_type == "manual",
                PredictionBatch.status == "completed",
                PredictionBatch.id != batch_id,
            )
            old_res = await session.execute(old_batch_stmt)
            old_batch_ids = [r[0] for r in old_res.all()]
            if old_batch_ids:
                del_stmt = sa_delete(PredictionResultRow).where(
                    PredictionResultRow.batch_id.in_(old_batch_ids)
                )
                del_result = await session.execute(del_stmt)
                logger.info(
                    "daily prediction: cleaned %s old results from %s previous batches",
                    del_result.rowcount, len(old_batch_ids),
                )
                # 清理旧批次记录本身
                batch_del_stmt = sa_delete(PredictionBatch).where(
                    PredictionBatch.id.in_(old_batch_ids)
                )
                await session.execute(batch_del_stmt)

            await svc.persist_sync_results(session, results, batch_id=batch_id, history_map=history_map)

            batch.status = "completed"
            batch.completed_at = datetime.now(timezone.utc)

            await write_background_audit(
                session,
                "daily_ai_prediction_completed",
                resource=f"batch:{batch_id}",
                detail={
                    "warehouses_count": len(wh_list),
                    "results_count": len(results),
                    "items_predicted": len(items),
                },
            )

            await session.commit()
            logger.info(
                "daily prediction finished: batch_id=%s, warehouses=%s, results=%s",
                batch_id, len(wh_list), len(results),
            )
        except Exception as e:
            logger.exception("daily prediction task failed batch_id=%s", batch_id)
            batch.status = "failed"
            batch.error_message = str(e)[:2000]
            batch.completed_at = datetime.now(timezone.utc)

            await write_background_audit(
                session,
                "daily_ai_prediction_failed",
                resource=f"batch:{batch_id}",
                detail={"error": str(e)[:500]},
            )

            await session.commit()


@celery_app.task(name="intelligent_prediction.run_daily_ai_prediction")
def run_daily_ai_prediction_task(batch_id: str) -> str:
    asyncio.run(_run_daily_prediction_async(batch_id))
    return batch_id


@celery_app.task(name="intelligent_prediction.run_prediction_batch")
def run_prediction_batch_task(batch_id: str) -> str:
    asyncio.run(_run_batch_async(batch_id))
    return batch_id

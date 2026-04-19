import json
from contextlib import contextmanager
import logging

import pymysql

from app import config

logger = logging.getLogger(__name__)

def get_mysql_config() -> dict:
    return {
        "host": config.MYSQL_HOST,
        "port": config.MYSQL_PORT,
        "user": config.MYSQL_USER,
        "password": config.MYSQL_PASSWORD,
        "database": config.MYSQL_DATABASE,
        "charset": config.MYSQL_CHARSET,
        "autocommit": True,
    }


def _get_mysql_config_without_db() -> dict:
    return {
        "host": config.MYSQL_HOST,
        "port": config.MYSQL_PORT,
        "user": config.MYSQL_USER,
        "password": config.MYSQL_PASSWORD,
        "charset": config.MYSQL_CHARSET,
        "autocommit": True,
    }


@contextmanager
def get_conn():
    """获取数据库连接的上下文管理器，退出时自动关闭连接"""
    conn = pymysql.connect(**get_mysql_config())
    try:
        yield conn
    finally:
        conn.close()


def create_database_if_not_exists():
    """自动创建数据库（如果不存在）"""
    connection = pymysql.connect(**_get_mysql_config_without_db())
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"CREATE DATABASE IF NOT EXISTS `{config.MYSQL_DATABASE}` "
                f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
            logger.info("数据库 '%s' 检查/创建完成", config.MYSQL_DATABASE)
    finally:
        connection.close()


TABLE_STATEMENTS = [
     # 用户表
    """
    CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY COMMENT '用户ID',
        username VARCHAR(50) NOT NULL UNIQUE COMMENT '用户名',
        hashed_password VARCHAR(255) NOT NULL COMMENT 'bcrypt 加密后的密码',
        real_name VARCHAR(50) COMMENT '真实姓名',
        role ENUM('admin', 'user') NOT NULL DEFAULT 'user' COMMENT '角色',
        phone VARCHAR(20) COMMENT '手机号',
        email VARCHAR(100) COMMENT '邮箱',
        is_active TINYINT(1) DEFAULT 1 COMMENT '是否启用',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户表';
    """,
    # 品类字典表
    """
    CREATE TABLE IF NOT EXISTS dict_categories (
        row_id INT AUTO_INCREMENT PRIMARY KEY COMMENT '行主键',
        category_id INT NOT NULL COMMENT '品类分组ID（多名称共用同一值）',
        name VARCHAR(50) NOT NULL UNIQUE COMMENT '品类名称',
        is_main TINYINT(1) DEFAULT 0 COMMENT '是否主品类（用于比价表展示）',
        is_active TINYINT(1) DEFAULT 1 COMMENT '是否启用',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_category_id (category_id),
        INDEX idx_category_main (category_id, is_main)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='品类字典表（多名称共用同一category_id）';
    """,
    # 库房类型字典（类型与颜色一对一，仓库通过 warehouse_type_id 关联）
    """
    CREATE TABLE IF NOT EXISTS dict_warehouse_types (
        id INT AUTO_INCREMENT PRIMARY KEY COMMENT '库房类型ID',
        name VARCHAR(50) NOT NULL UNIQUE COMMENT '类型名称',
        color_config JSON DEFAULT NULL COMMENT '颜色配置（JSON），与类型唯一绑定',
        is_active TINYINT(1) DEFAULT 1 COMMENT '是否启用',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_wh_type_active (is_active)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='库房类型字典（类型-颜色一对一）';
    """,
    # 仓库字典表
    """
    CREATE TABLE IF NOT EXISTS dict_warehouses (
        id INT AUTO_INCREMENT PRIMARY KEY COMMENT '仓库ID',
        name VARCHAR(100) NOT NULL UNIQUE COMMENT '仓库名称',
        address VARCHAR(500) DEFAULT NULL COMMENT '地址',
        warehouse_type_id INT DEFAULT NULL COMMENT '库房类型ID（颜色由类型表带出）',
        is_active TINYINT(1) DEFAULT 1 COMMENT '是否启用',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        CONSTRAINT fk_wh_warehouse_type FOREIGN KEY (warehouse_type_id)
            REFERENCES dict_warehouse_types (id) ON UPDATE CASCADE ON DELETE SET NULL,
        INDEX idx_wh_warehouse_type (warehouse_type_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='仓库字典表';
    """,
    # 冶炼厂字典表
    """
    CREATE TABLE IF NOT EXISTS dict_factories (
        id INT AUTO_INCREMENT PRIMARY KEY COMMENT '冶炼厂ID',
        name VARCHAR(100) NOT NULL UNIQUE COMMENT '冶炼厂名称',
        address VARCHAR(500) DEFAULT NULL COMMENT '冶炼厂地址',
        is_active TINYINT(1) DEFAULT 1 COMMENT '是否启用',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='冶炼厂字典表';
    """,
    # 运费价格表
    """
    CREATE TABLE IF NOT EXISTS freight_rates (
        id INT AUTO_INCREMENT PRIMARY KEY,
        factory_id INT NOT NULL COMMENT '冶炼厂ID',
        warehouse_id INT NOT NULL COMMENT '仓库ID',
        price_per_ton DECIMAL(10, 2) NOT NULL COMMENT '每吨运费（元）',
        effective_date DATE NOT NULL COMMENT '生效日期',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        CONSTRAINT fk_freight_factory FOREIGN KEY (factory_id) REFERENCES dict_factories (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        CONSTRAINT fk_freight_warehouse FOREIGN KEY (warehouse_id) REFERENCES dict_warehouses (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        UNIQUE KEY uk_factory_warehouse_date (factory_id, warehouse_id, effective_date),
        INDEX idx_effective_date (effective_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='运费价格表';
    """,
    # 报价表元数据表（存储VLM提取的完整原始信息）
    """
    CREATE TABLE IF NOT EXISTS quote_table_metadata (
        id INT AUTO_INCREMENT PRIMARY KEY COMMENT '报价表ID',
        factory_id INT NOT NULL COMMENT '冶炼厂ID',
        quote_date DATE NOT NULL COMMENT '报价日期',
        execution_date VARCHAR(50) COMMENT '执行日期（如：2026年3月17日）',
        doc_title VARCHAR(200) COMMENT '文档标题',
        subtitle VARCHAR(200) COMMENT '副标题',
        valid_period VARCHAR(100) COMMENT '有效期',
        price_unit VARCHAR(50) DEFAULT '元/吨' COMMENT '价格单位',
        headers JSON COMMENT '表头列表',
        footer_notes JSON COMMENT '页脚备注列表',
        footer_notes_raw TEXT COMMENT '页脚备注原始文本',
        brand_specifications TEXT COMMENT '品牌规格说明',
        policies JSON COMMENT '政策信息',
        raw_full_text LONGTEXT COMMENT '原始完整识别文本',
        source_image VARCHAR(500) COMMENT '来源图片文件名',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        CONSTRAINT fk_metadata_factory FOREIGN KEY (factory_id) REFERENCES dict_factories (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        UNIQUE KEY uk_factory_quote_date (factory_id, quote_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='报价表元数据表（VLM全量提取）';
    """,
    # 冶炼厂税率表（用户手动维护，按冶炼厂+税率存一行）
    """
    CREATE TABLE IF NOT EXISTS factory_tax_rates (
        id INT AUTO_INCREMENT PRIMARY KEY,
        factory_id INT NOT NULL COMMENT '冶炼厂ID',
        tax_type VARCHAR(20) NOT NULL COMMENT '税率类型：1pct/3pct/13pct',
        tax_rate DECIMAL(6, 4) NOT NULL COMMENT '税率值，如 0.03 表示3%',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        CONSTRAINT fk_tax_factory FOREIGN KEY (factory_id) REFERENCES dict_factories (id) ON UPDATE CASCADE ON DELETE CASCADE,
        UNIQUE KEY uk_factory_tax_type (factory_id, tax_type)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='冶炼厂税率表';
    """,
    # 报价明细表
    """
    CREATE TABLE IF NOT EXISTS quote_details (
        id INT AUTO_INCREMENT PRIMARY KEY,
        quote_date DATE NOT NULL COMMENT '报价日期',
        factory_id INT NOT NULL COMMENT '冶炼厂ID',
        category_name VARCHAR(100) NOT NULL COMMENT '品类名称（关联dict_categories.name）',
        metadata_id INT COMMENT '关联报价表元数据ID',
        unit_price DECIMAL(10, 2) COMMENT '不含税基准价（元/吨）',
        price_1pct_vat DECIMAL(10, 2) COMMENT '1%增值税价格',
        price_3pct_vat DECIMAL(10, 2) COMMENT '3%增值税价格',
        price_13pct_vat DECIMAL(10, 2) COMMENT '13%增值税价格',
        price_normal_invoice DECIMAL(10, 2) COMMENT '普通发票价格',
        price_reverse_invoice DECIMAL(10, 2) COMMENT '反向发票价格',
        price_field_sources JSON NULL COMMENT '各价格字段来源：键为列名，值为原数据/换算',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        CONSTRAINT fk_detail_factory FOREIGN KEY (factory_id) REFERENCES dict_factories (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        CONSTRAINT fk_detail_metadata FOREIGN KEY (metadata_id) REFERENCES quote_table_metadata (id) ON UPDATE CASCADE ON DELETE SET NULL,
        UNIQUE KEY uk_factory_category_date (factory_id, category_name, quote_date),
        INDEX idx_quote_date (quote_date),
        INDEX idx_factory_id (factory_id),
        INDEX idx_category_name (category_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='报价明细表';
    """,
    # 仓库库存表（预留）
    """
    CREATE TABLE IF NOT EXISTS warehouse_inventories (
        id INT AUTO_INCREMENT PRIMARY KEY,
        warehouse_id INT NOT NULL COMMENT '仓库ID',
        category_id INT NOT NULL COMMENT '品类行ID（关联dict_categories.row_id）',
        available_tons DECIMAL(10, 3) NOT NULL DEFAULT 0 COMMENT '当前可用吨数',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        CONSTRAINT fk_inventory_warehouse FOREIGN KEY (warehouse_id) REFERENCES dict_warehouses (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        CONSTRAINT fk_inventory_category FOREIGN KEY (category_id) REFERENCES dict_categories (row_id) ON UPDATE CASCADE ON DELETE RESTRICT,
        UNIQUE KEY uk_inventory_warehouse_category (warehouse_id, category_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='仓库库存表（预留）';
    """,
    # 冶炼厂需求主表（预留）
    """
    CREATE TABLE IF NOT EXISTS factory_demands (
        id INT AUTO_INCREMENT PRIMARY KEY,
        factory_id INT NOT NULL COMMENT '冶炼厂ID',
        demand_date DATE NOT NULL COMMENT '需求日期',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        CONSTRAINT fk_demand_factory FOREIGN KEY (factory_id) REFERENCES dict_factories (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        UNIQUE KEY uk_factory_demand_date (factory_id, demand_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='冶炼厂需求主表（预留）';
    """,
    # 冶炼厂需求明细表（预留）
    """
    CREATE TABLE IF NOT EXISTS factory_demand_items (
        id INT AUTO_INCREMENT PRIMARY KEY,
        demand_id INT NOT NULL COMMENT '需求主表ID',
        category_id INT NOT NULL COMMENT '品类行ID（关联dict_categories.row_id）',
        required_tons DECIMAL(10, 3) NOT NULL DEFAULT 0 COMMENT '需求吨数',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        CONSTRAINT fk_demand_item_demand FOREIGN KEY (demand_id) REFERENCES factory_demands (id) ON UPDATE CASCADE ON DELETE CASCADE,
        CONSTRAINT fk_demand_item_category FOREIGN KEY (category_id) REFERENCES dict_categories (row_id) ON UPDATE CASCADE ON DELETE RESTRICT,
        UNIQUE KEY uk_demand_category (demand_id, category_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='冶炼厂需求明细表（预留）';
    """,
    # 图片鉴伪检测历史（保留策略由应用层按天数清理）
    """
    CREATE TABLE IF NOT EXISTS ai_detection_history (
        id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间（UTC）',
        mode VARCHAR(24) NOT NULL COMMENT 'sync_v1=同步单框 | async_v3=异步任务',
        task_id VARCHAR(64) NULL COMMENT '异步任务 UUID，同步为空',
        original_filename VARCHAR(512) NULL COMMENT '上传原始文件名',
        bbox JSON NULL COMMENT '检测框或自动模式说明',
        status VARCHAR(32) NOT NULL COMMENT 'COMPLETED | FAILED',
        outcome_json JSON NOT NULL COMMENT '结果摘要：result / multi_results / error_msg',
        stored_image VARCHAR(255) NULL COMMENT '归档图文件名（置于 ai_detection_history_images/）',
        INDEX idx_ai_hist_created (created_at),
        INDEX idx_ai_hist_task (task_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='AI图片鉴伪历史记录';
    """,
    # 智能送货量预测（与 intelligent_prediction ORM 表名一致）
    """
    CREATE TABLE IF NOT EXISTS pd_ip_delivery_records (
        id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
        regional_manager VARCHAR(255) NOT NULL COMMENT '大区经理',
        smelter VARCHAR(100) DEFAULT NULL COMMENT '冶炼厂',
        warehouse VARCHAR(255) NOT NULL COMMENT '仓库',
        delivery_date DATE NOT NULL COMMENT '送货日期',
        product_variety VARCHAR(255) NOT NULL COMMENT '品种',
        weight DECIMAL(18,4) NOT NULL COMMENT '重量',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        INDEX idx_ip_delivery_date (delivery_date),
        INDEX idx_ip_warehouse (warehouse),
        INDEX idx_ip_product_variety (product_variety),
        INDEX idx_ip_regional_manager (regional_manager),
        INDEX idx_ip_smelter (smelter)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='智能预测-送货历史';
    """,
    """
    CREATE TABLE IF NOT EXISTS pd_ip_prediction_batches (
        id CHAR(36) NOT NULL PRIMARY KEY COMMENT '批次UUID字符串',
        status VARCHAR(32) NOT NULL DEFAULT 'pending' COMMENT '状态',
        celery_task_id VARCHAR(255) DEFAULT NULL COMMENT 'Celery任务ID',
        error_message TEXT COMMENT '错误信息',
        export_file_path VARCHAR(1024) DEFAULT NULL COMMENT '导出Excel路径',
        meta JSON DEFAULT NULL COMMENT '请求元数据',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        completed_at TIMESTAMP NULL DEFAULT NULL COMMENT '完成时间',
        INDEX idx_ip_batch_status (status),
        INDEX idx_ip_batch_celery (celery_task_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='智能预测-批次任务';
    """,
    """
    CREATE TABLE IF NOT EXISTS pd_ip_prediction_results (
        id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
        batch_id CHAR(36) DEFAULT NULL COMMENT '批次ID',
        regional_manager VARCHAR(255) DEFAULT NULL COMMENT '大区经理',
        warehouse VARCHAR(255) NOT NULL COMMENT '仓库',
        product_variety VARCHAR(255) NOT NULL COMMENT '品种',
        smelter VARCHAR(100) DEFAULT NULL COMMENT '冶炼厂',
        target_date DATE NOT NULL COMMENT '预测目标日',
        predicted_weight DECIMAL(18,4) NOT NULL COMMENT '预测重量',
        confidence VARCHAR(32) NOT NULL DEFAULT 'medium' COMMENT '信心',
        warnings JSON DEFAULT NULL COMMENT '警告列表',
        provider_used VARCHAR(64) DEFAULT NULL COMMENT '供应商',
        latency_ms DECIMAL(12,4) DEFAULT NULL COMMENT '延迟毫秒',
        cost_usd DECIMAL(12,6) DEFAULT NULL COMMENT '成本美元',
        raw_response_excerpt TEXT COMMENT '原始摘要/解析备注',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        INDEX idx_ip_res_batch (batch_id),
        INDEX idx_ip_res_warehouse (warehouse),
        INDEX idx_ip_res_variety (product_variety),
        INDEX idx_ip_res_smelter (smelter),
        INDEX idx_ip_res_target_date (target_date),
        CONSTRAINT fk_ip_res_batch FOREIGN KEY (batch_id) REFERENCES pd_ip_prediction_batches(id) ON DELETE SET NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='智能预测-结果明细';
    """,
    """
    CREATE TABLE IF NOT EXISTS pd_ip_operation_audit (
        id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
        user_id BIGINT DEFAULT NULL COMMENT '用户ID',
        user_label VARCHAR(255) DEFAULT NULL COMMENT '用户标识或姓名',
        action VARCHAR(64) NOT NULL COMMENT '操作类型',
        resource VARCHAR(128) DEFAULT NULL COMMENT '资源简述',
        detail JSON DEFAULT NULL COMMENT '详情JSON',
        client_ip VARCHAR(64) DEFAULT NULL COMMENT '客户端IP',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        INDEX idx_ip_audit_action (action),
        INDEX idx_ip_audit_created (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='智能预测-操作审计';
    """,
]


def ensure_quote_details_price_field_sources_column() -> None:
    """已有库升级：为 quote_details 增加 price_field_sources（新建库已由 CREATE TABLE 包含）。"""
    config_dict = get_mysql_config()
    connection = pymysql.connect(**config_dict)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema = DATABASE() AND table_name = 'quote_details' "
                "AND column_name = 'price_field_sources'"
            )
            if cursor.fetchone()[0] == 0:
                cursor.execute(
                    "ALTER TABLE quote_details ADD COLUMN price_field_sources JSON NULL "
                    "COMMENT '各价格字段来源：键为列名，值为原数据/换算' "
                    "AFTER price_reverse_invoice"
                )
                logger.info("已为 quote_details 添加 price_field_sources 列")
        connection.commit()
    finally:
        connection.close()


def ensure_pd_ip_prediction_results_smelter_column() -> None:
    """已有库升级：为 pd_ip_prediction_results 补全 smelter（新建库已由 CREATE TABLE 包含）。"""
    config_dict = get_mysql_config()
    connection = pymysql.connect(**config_dict)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE 'pd_ip_prediction_results'")
            if cursor.fetchone() is None:
                return
            cursor.execute("SHOW COLUMNS FROM pd_ip_prediction_results LIKE 'smelter'")
            if cursor.fetchone() is not None:
                return
            cursor.execute(
                "ALTER TABLE pd_ip_prediction_results "
                "ADD COLUMN smelter VARCHAR(100) DEFAULT NULL COMMENT '冶炼厂' "
                "AFTER product_variety"
            )
            try:
                cursor.execute(
                    "ALTER TABLE pd_ip_prediction_results ADD INDEX idx_ip_res_smelter (smelter)"
                )
            except Exception:
                pass
            logger.info("已为 pd_ip_prediction_results 添加 smelter 列")
        connection.commit()
    finally:
        connection.close()


def ensure_pd_ip_delivery_records_smelter_column() -> None:
    """已有库升级：为 pd_ip_delivery_records 补全 smelter。"""
    config_dict = get_mysql_config()
    connection = pymysql.connect(**config_dict)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE 'pd_ip_delivery_records'")
            if cursor.fetchone() is None:
                return
            cursor.execute("SHOW COLUMNS FROM pd_ip_delivery_records LIKE 'smelter'")
            if cursor.fetchone() is not None:
                return
            cursor.execute(
                "ALTER TABLE pd_ip_delivery_records "
                "ADD COLUMN smelter VARCHAR(100) DEFAULT NULL COMMENT '冶炼厂' "
                "AFTER regional_manager"
            )
            try:
                cursor.execute(
                    "ALTER TABLE pd_ip_delivery_records ADD INDEX idx_ip_smelter (smelter)"
                )
            except Exception:
                pass
            logger.info("已为 pd_ip_delivery_records 添加 smelter 列")
        connection.commit()
    finally:
        connection.close()


def ensure_dict_warehouses_extended_columns() -> None:
    """已有库升级：为 dict_warehouses 仅增加 address（类型/颜色已迁移至 dict_warehouse_types）。"""
    config_dict = get_mysql_config()
    connection = pymysql.connect(**config_dict)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE 'dict_warehouses'")
            if cursor.fetchone() is None:
                return

            def _has_col(table: str, col: str) -> bool:
                cursor.execute(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema = DATABASE() AND table_name = %s "
                    "AND column_name = %s",
                    (table, col),
                )
                return cursor.fetchone()[0] > 0

            if not _has_col("dict_warehouses", "address"):
                cursor.execute(
                    "ALTER TABLE dict_warehouses ADD COLUMN address VARCHAR(500) DEFAULT NULL "
                    "COMMENT '地址' AFTER name"
                )
                logger.info("已为 dict_warehouses 添加 address 列")
        connection.commit()
    finally:
        connection.close()


def ensure_dict_warehouse_types_migration() -> None:
    """库房类型表 + 仓库 warehouse_type_id；从旧列 warehouse_type/color_config 迁移后删除旧列。"""
    config_dict = get_mysql_config()
    connection = pymysql.connect(**config_dict)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS dict_warehouse_types (
                    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '库房类型ID',
                    name VARCHAR(50) NOT NULL UNIQUE COMMENT '类型名称',
                    color_config JSON DEFAULT NULL COMMENT '颜色配置（JSON），与类型唯一绑定',
                    is_active TINYINT(1) DEFAULT 1 COMMENT '是否启用',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_wh_type_active (is_active)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='库房类型字典（类型-颜色一对一）';
                """
            )

            cursor.execute("SHOW TABLES LIKE 'dict_warehouses'")
            if cursor.fetchone() is None:
                connection.commit()
                return

            def _has_col(col: str) -> bool:
                cursor.execute(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema = DATABASE() AND table_name = 'dict_warehouses' "
                    "AND column_name = %s",
                    (col,),
                )
                return cursor.fetchone()[0] > 0

            def _has_fk_wh_type() -> bool:
                cursor.execute(
                    "SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS "
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'dict_warehouses' "
                    "AND CONSTRAINT_TYPE = 'FOREIGN KEY' AND CONSTRAINT_NAME = 'fk_wh_warehouse_type'"
                )
                return cursor.fetchone()[0] > 0

            if not _has_col("warehouse_type_id"):
                after = "address" if _has_col("address") else "name"
                cursor.execute(
                    f"ALTER TABLE dict_warehouses ADD COLUMN warehouse_type_id INT DEFAULT NULL "
                    f"COMMENT '库房类型ID' AFTER {after}"
                )
                logger.info("已为 dict_warehouses 添加 warehouse_type_id 列")

            has_varchar = _has_col("warehouse_type")
            has_color = _has_col("color_config")

            if has_varchar:
                cursor.execute(
                    "SELECT DISTINCT TRIM(warehouse_type) AS t FROM dict_warehouses "
                    "WHERE warehouse_type IS NOT NULL AND TRIM(warehouse_type) <> ''"
                )
                for (tname,) in cursor.fetchall():
                    if not tname:
                        continue
                    cursor.execute(
                        "INSERT IGNORE INTO dict_warehouse_types (name, is_active) VALUES (%s, 1)",
                        (tname,),
                    )
                cursor.execute(
                    """
                    UPDATE dict_warehouses w
                    INNER JOIN dict_warehouse_types t ON t.name = TRIM(w.warehouse_type)
                    SET w.warehouse_type_id = t.id
                    WHERE w.warehouse_type_id IS NULL AND w.warehouse_type IS NOT NULL
                    """
                )
                logger.info("已从旧 warehouse_type 列回填 warehouse_type_id")

            if has_color and _has_col("warehouse_type_id"):
                cursor.execute(
                    "SELECT id, warehouse_type_id, color_config FROM dict_warehouses "
                    "WHERE color_config IS NOT NULL AND warehouse_type_id IS NOT NULL"
                )
                for _wh_id, tid, cc in cursor.fetchall():
                    if cc is None or tid is None:
                        continue
                    cursor.execute(
                        "SELECT color_config FROM dict_warehouse_types WHERE id = %s", (tid,)
                    )
                    row = cursor.fetchone()
                    if not row or row[0] is not None:
                        continue
                    if isinstance(cc, (dict, list)):
                        cc_payload = json.dumps(cc, ensure_ascii=False)
                        cursor.execute(
                            "UPDATE dict_warehouse_types SET color_config = CAST(%s AS JSON) "
                            "WHERE id = %s",
                            (cc_payload, tid),
                        )
                    else:
                        cursor.execute(
                            "UPDATE dict_warehouse_types SET color_config = %s WHERE id = %s",
                            (cc, tid),
                        )
                logger.info("已将仓库上旧 color_config 合并到对应库房类型（仅类型色为空时）")

            if not _has_fk_wh_type():
                try:
                    cursor.execute(
                        """
                        ALTER TABLE dict_warehouses
                        ADD CONSTRAINT fk_wh_warehouse_type
                        FOREIGN KEY (warehouse_type_id) REFERENCES dict_warehouse_types(id)
                        ON UPDATE CASCADE ON DELETE SET NULL
                        """
                    )
                    logger.info("已为 dict_warehouses 添加外键 fk_wh_warehouse_type")
                except Exception:
                    logger.exception("添加 fk_wh_warehouse_type 失败")

            if has_varchar:
                try:
                    cursor.execute("ALTER TABLE dict_warehouses DROP COLUMN warehouse_type")
                    logger.info("已删除废弃列 dict_warehouses.warehouse_type")
                except Exception:
                    logger.exception("删除 warehouse_type 列失败")

            if has_color:
                try:
                    cursor.execute("ALTER TABLE dict_warehouses DROP COLUMN color_config")
                    logger.info("已删除废弃列 dict_warehouses.color_config")
                except Exception:
                    logger.exception("删除 color_config 列失败")

            try:
                cursor.execute(
                    "ALTER TABLE dict_warehouses ADD INDEX idx_wh_warehouse_type (warehouse_type_id)"
                )
            except Exception:
                pass

        connection.commit()
    finally:
        connection.close()


def ensure_dict_factories_address_column() -> None:
    """已有库升级：为 dict_factories 增加 address（冶炼厂地址）。"""
    config_dict = get_mysql_config()
    connection = pymysql.connect(**config_dict)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE 'dict_factories'")
            if cursor.fetchone() is None:
                return
            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema = DATABASE() AND table_name = 'dict_factories' "
                "AND column_name = 'address'"
            )
            if cursor.fetchone()[0] > 0:
                return
            cursor.execute(
                "ALTER TABLE dict_factories ADD COLUMN address VARCHAR(500) DEFAULT NULL "
                "COMMENT '冶炼厂地址' AFTER name"
            )
            logger.info("已为 dict_factories 添加 address 列")
        connection.commit()
    finally:
        connection.close()


def ensure_ai_detection_history_stored_image_column() -> None:
    """已有库升级：为 ai_detection_history 增加 stored_image（新建库已由 CREATE TABLE 包含）。"""
    config_dict = get_mysql_config()
    connection = pymysql.connect(**config_dict)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema = DATABASE() AND table_name = 'ai_detection_history' "
                "AND column_name = 'stored_image'"
            )
            if cursor.fetchone()[0] == 0:
                cursor.execute(
                    "ALTER TABLE ai_detection_history ADD COLUMN stored_image VARCHAR(255) NULL "
                    "COMMENT '归档图文件名' AFTER outcome_json"
                )
                logger.info("已为 ai_detection_history 添加 stored_image 列")
        connection.commit()
    finally:
        connection.close()


def create_tables() -> None:
    create_database_if_not_exists()
    config_dict = get_mysql_config()
    connection = pymysql.connect(**config_dict)
    try:
        with connection.cursor() as cursor:
            for statement in TABLE_STATEMENTS:
                cursor.execute(statement)
        connection.commit()
        logger.info("所有数据表创建完成")
    finally:
        connection.close()
    try:
        ensure_quote_details_price_field_sources_column()
    except Exception:
        logger.exception("检查/添加 quote_details.price_field_sources 失败")
    try:
        ensure_ai_detection_history_stored_image_column()
    except Exception:
        logger.exception("检查/添加 ai_detection_history.stored_image 失败")
    try:
        ensure_pd_ip_delivery_records_smelter_column()
    except Exception:
        logger.exception("检查/添加 pd_ip_delivery_records.smelter 失败")
    try:
        ensure_pd_ip_prediction_results_smelter_column()
    except Exception:
        logger.exception("检查/添加 pd_ip_prediction_results.smelter 失败")
    try:
        ensure_dict_warehouses_extended_columns()
    except Exception:
        logger.exception("检查/添加 dict_warehouses 扩展列失败")
    try:
        ensure_dict_warehouse_types_migration()
    except Exception:
        logger.exception("库房类型表/warehouse_type_id 迁移失败")
    try:
        ensure_dict_factories_address_column()
    except Exception:
        logger.exception("检查/添加 dict_factories.address 失败")


def init_default_data() -> None:
    """插入默认的仓库和冶炼厂数据"""
    connection = pymysql.connect(**get_mysql_config())
    try:
        with connection.cursor() as cursor:
            # 插入默认仓库
            cursor.execute(
                "INSERT IGNORE INTO dict_warehouses (id, name, is_active) VALUES "
                "(1, '默认仓库', 1)"
            )
            # 插入默认冶炼厂
            cursor.execute(
                "INSERT IGNORE INTO dict_factories (id, name, is_active) VALUES "
                "(1, '默认冶炼厂', 1)"
            )
        connection.commit()
        logger.info("默认数据初始化完成")
    finally:
        connection.close()


if __name__ == "__main__":
    create_tables()

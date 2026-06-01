import json
from contextlib import contextmanager
import logging
import sys
from pathlib import Path

# 从 app/ 目录执行 `python database.py` 时，sys.path 不含项目根，无法解析 `app` 包
if __name__ == "__main__":
    _root = Path(__file__).resolve().parent.parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

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
        role VARCHAR(32) NOT NULL DEFAULT 'user' COMMENT '角色代码，见 role_definitions.code',
        phone VARCHAR(20) COMMENT '手机号',
        email VARCHAR(100) COMMENT '邮箱',
        is_active TINYINT(1) DEFAULT 1 COMMENT '是否启用',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户表';
    """,
    # 可配置角色（与 users.role、role_templates.role 一致）
    """
    CREATE TABLE IF NOT EXISTS role_definitions (
        id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
        code VARCHAR(32) NOT NULL UNIQUE COMMENT '角色代码，写入 users.role',
        name VARCHAR(64) NOT NULL COMMENT '显示名称',
        description VARCHAR(255) DEFAULT NULL COMMENT '说明',
        sort_order INT NOT NULL DEFAULT 0 COMMENT '排序，升序展示',
        is_system TINYINT(1) NOT NULL DEFAULT 0 COMMENT '1=内置不可删除',
        is_active TINYINT(1) NOT NULL DEFAULT 1 COMMENT '1=启用，可分配给新用户',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_role_definitions_active (is_active),
        INDEX idx_role_definitions_sort (sort_order)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色定义（配置管理）';
    """,
    # 权限字段定义（动态增删列时同步 user_permissions 与角色模板 JSON）
    """
    CREATE TABLE IF NOT EXISTS permission_definitions (
        field_name VARCHAR(64) PRIMARY KEY COMMENT '权限字段名（如 perm_xxx）',
        label VARCHAR(64) NOT NULL COMMENT '显示名称',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='权限字段定义表';
    """,
    # 角色权限模板（JSON：字段名 -> 0/1；初始仅 seed 管理员模板且全为 0，由接口维护）
    """
    CREATE TABLE IF NOT EXISTS role_templates (
        id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
        role VARCHAR(32) NOT NULL UNIQUE COMMENT '与 users.role 对应：admin / user',
        template_json TEXT NOT NULL COMMENT '权限模板 JSON',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色权限模板表';
    """,
    # 用户细粒度权限（动态列与 permission_definitions 对齐）
    """
    CREATE TABLE IF NOT EXISTS user_permissions (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL COMMENT '用户ID',
        role VARCHAR(32) NOT NULL DEFAULT 'user' COMMENT '权限行角色，与 users.role 对齐',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_user_permissions_user_id (user_id),
        INDEX idx_user_permissions_role (role),
        CONSTRAINT fk_user_permissions_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户权限配置表';
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
        province VARCHAR(64) DEFAULT NULL COMMENT '省',
        city VARCHAR(64) DEFAULT NULL COMMENT '市',
        district VARCHAR(64) DEFAULT NULL COMMENT '区县',
        address VARCHAR(500) DEFAULT NULL COMMENT '详细地址',
        warehouse_type_id INT DEFAULT NULL COMMENT '库房类型ID（类型颜色见 dict_warehouse_types）',
        color_config JSON DEFAULT NULL COMMENT '仓库独立颜色配置（JSON），可与库房类型颜色并存',
        longitude DECIMAL(11, 8) DEFAULT NULL COMMENT '经度',
        latitude DECIMAL(10, 8) DEFAULT NULL COMMENT '纬度',
        contact_name VARCHAR(64) DEFAULT NULL COMMENT '库房联系人',
        contact_phone VARCHAR(32) DEFAULT NULL COMMENT '电话',
        hazardous_waste_license_qty DECIMAL(14, 4) DEFAULT NULL COMMENT '危废经营许可数量',
        monthly_avg_receipt_ton DECIMAL(14, 4) DEFAULT NULL COMMENT '月均收货(吨)',
        current_inventory_ton DECIMAL(14, 4) DEFAULT NULL COMMENT '当前库存(吨)',
        receipt_price_per_ton DECIMAL(14, 4) DEFAULT NULL COMMENT '收货价格(元/吨)',
        freight_amount DECIMAL(14, 4) DEFAULT NULL COMMENT '运费参考(元)',
        is_active TINYINT(1) DEFAULT 1 COMMENT '是否启用',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        CONSTRAINT fk_wh_warehouse_type FOREIGN KEY (warehouse_type_id)
            REFERENCES dict_warehouse_types (id) ON UPDATE CASCADE ON DELETE SET NULL,
        INDEX idx_wh_warehouse_type (warehouse_type_id),
        INDEX idx_wh_geo_region (province, city, district)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='仓库字典表';
    """,
    # 库房单向关联边表（有向图：from -> to，一个源可连多条出边）
    """
    CREATE TABLE IF NOT EXISTS dict_warehouse_links (
        id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '边ID',
        from_warehouse_id INT NOT NULL COMMENT '源库房（出边起点）',
        to_warehouse_id INT NOT NULL COMMENT '对标库房（单向指向终点）',
        tier_price_spread JSON DEFAULT NULL COMMENT '阶梯价差（JSON，可按距离区间维护价差）',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        UNIQUE KEY uk_wh_link_from_to (from_warehouse_id, to_warehouse_id),
        INDEX idx_wh_link_from (from_warehouse_id),
        INDEX idx_wh_link_to (to_warehouse_id),
        CONSTRAINT fk_wh_link_from FOREIGN KEY (from_warehouse_id)
            REFERENCES dict_warehouses (id) ON DELETE CASCADE ON UPDATE CASCADE,
        CONSTRAINT fk_wh_link_to FOREIGN KEY (to_warehouse_id)
            REFERENCES dict_warehouses (id) ON DELETE CASCADE ON UPDATE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='库房单向关联（有向图边）';
    """,
    # 冶炼厂字典表
    """
    CREATE TABLE IF NOT EXISTS dict_factories (
        id INT AUTO_INCREMENT PRIMARY KEY COMMENT '冶炼厂ID',
        name VARCHAR(100) NOT NULL UNIQUE COMMENT '冶炼厂名称',
        province VARCHAR(64) DEFAULT NULL COMMENT '省',
        city VARCHAR(64) DEFAULT NULL COMMENT '市',
        district VARCHAR(64) DEFAULT NULL COMMENT '区县',
        address VARCHAR(500) DEFAULT NULL COMMENT '冶炼厂地址',
        color_config JSON DEFAULT NULL COMMENT '标记颜色等 JSON',
        longitude DECIMAL(11, 8) DEFAULT NULL COMMENT '经度',
        latitude DECIMAL(10, 8) DEFAULT NULL COMMENT '纬度',
        use_xunrongbao TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否循融宝发货（启用时每吨货物单价加价）',
        is_active TINYINT(1) DEFAULT 1 COMMENT '是否启用',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_df_geo_region (province, city, district)
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
    # 库房当前库存快照（库房+品类+日期；页面展示每品类最新一条）
    """
    CREATE TABLE IF NOT EXISTS warehouse_inventory_snapshots (
        id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
        warehouse_id INT NOT NULL COMMENT '库房ID',
        category_id INT NOT NULL COMMENT '品类分组ID（dict_categories.category_id）',
        inventory_ton DECIMAL(14, 4) NOT NULL COMMENT '当前库存(吨)',
        inventory_date DATE NOT NULL COMMENT '库存日期',
        source VARCHAR(32) NOT NULL DEFAULT 'import' COMMENT '来源：import/manual',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        CONSTRAINT fk_wis_warehouse FOREIGN KEY (warehouse_id) REFERENCES dict_warehouses (id)
            ON UPDATE CASCADE ON DELETE CASCADE,
        UNIQUE KEY uk_wis_wh_cat_date (warehouse_id, category_id, inventory_date),
        INDEX idx_wis_wh_cat_date (warehouse_id, category_id, inventory_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='库房库存快照（按库房、品类、日期）';
    """,
    # 库房按品种收货价格（回收单价维护）
    """
    CREATE TABLE IF NOT EXISTS warehouse_category_receipt_prices (
        id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
        warehouse_id INT NOT NULL COMMENT '库房ID',
        category_id INT NOT NULL COMMENT '品类分组ID（dict_categories.category_id）',
        price_per_ton DECIMAL(14, 4) NOT NULL COMMENT '库房回收单价(元/吨)',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        CONSTRAINT fk_wcrp_warehouse FOREIGN KEY (warehouse_id) REFERENCES dict_warehouses (id)
            ON UPDATE CASCADE ON DELETE CASCADE,
        UNIQUE KEY uk_wcrp_wh_cat (warehouse_id, category_id),
        INDEX idx_wcrp_wh (warehouse_id),
        INDEX idx_wcrp_cat (category_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='库房按品种收货价格';
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
        mode VARCHAR(24) NOT NULL COMMENT 'sync_v1 | async_v3 | rule_checks | rule_pixel_overlap | rule_timestamp',
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
        warehouse_address VARCHAR(512) DEFAULT NULL COMMENT '仓库地址',
        smelter_address VARCHAR(512) DEFAULT NULL COMMENT '冶炼厂地址',
        delivery_date DATE NOT NULL COMMENT '送货日期',
        product_variety VARCHAR(255) NOT NULL COMMENT '品种',
        weight DECIMAL(18,4) NOT NULL COMMENT '重量',
        cn_is_workday TINYINT(1) DEFAULT NULL COMMENT '是否中国工作日：与导入节假日列一致',
        cn_calendar_label VARCHAR(128) DEFAULT NULL COMMENT '导入节假日列：仅「是」非工作日或「否」工作日',
        weather_json JSON DEFAULT NULL COMMENT '天气API返回摘要',
        import_weather VARCHAR(64) DEFAULT NULL COMMENT '导入天气简述，空按晴',
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
        analysis TEXT DEFAULT NULL COMMENT '预测依据文案',
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
    """
    CREATE TABLE IF NOT EXISTS pd_ip_lead_market_prices (
        id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
        price_date DATE NOT NULL COMMENT '铅价/行情价日期',
        lead_price DECIMAL(18, 4) NOT NULL COMMENT '铅价（元/吨，行情基准）',
        remark VARCHAR(255) DEFAULT NULL COMMENT '备注或数据来源',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        UNIQUE KEY uk_ip_lead_price_date (price_date),
        INDEX idx_ip_lead_price_date (price_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='智能预测-铅价/行情价历史';
    """,
    # 省份「对标城市」定价历史（同一省份多条记录时默认取 price_date 最新，同日取 id 最大）
    """
    CREATE TABLE IF NOT EXISTS pd_province_benchmark_prices (
        id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
        province VARCHAR(64) NOT NULL COMMENT '省份（与 dict_warehouses.province 展示口径一致）',
        benchmark_city VARCHAR(128) NOT NULL COMMENT '对标城市',
        benchmark_price DECIMAL(18, 4) NOT NULL COMMENT '对标城市定价',
        price_date DATE NOT NULL COMMENT '定价日期',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '上传时间',
        INDEX idx_pbp_province_date (province, price_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='省份对标城市定价历史';
    """,
    # 冶炼厂标定价格历史（按厂+日期；当前有效取最新日期同日最大 id）
    """
    CREATE TABLE IF NOT EXISTS pd_smelter_calibration_prices (
        id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
        factory_id INT NOT NULL COMMENT '冶炼厂ID',
        calibration_price DECIMAL(18, 4) NOT NULL COMMENT '标定价格',
        price_date DATE NOT NULL COMMENT '定价日期',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '上传时间',
        CONSTRAINT fk_scp_factory FOREIGN KEY (factory_id) REFERENCES dict_factories (id)
            ON UPDATE CASCADE ON DELETE RESTRICT,
        INDEX idx_scp_factory_date (factory_id, price_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='冶炼厂标定价格历史';
    """,
    # SMM 1#铅锭公开页参考价（按定价日期唯一；均价=(最低+最高)/2）
    """
    CREATE TABLE IF NOT EXISTS pd_smm_lead_reference_prices (
        id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
        product_name VARCHAR(64) NOT NULL DEFAULT 'SMM 1#铅锭' COMMENT '品种名称',
        price_low DECIMAL(18, 4) NOT NULL COMMENT '区间最低价',
        price_high DECIMAL(18, 4) NOT NULL COMMENT '区间最高价',
        average_price DECIMAL(18, 4) NOT NULL COMMENT '均价=(最低+最高)/2',
        unit VARCHAR(16) NOT NULL DEFAULT '元/吨' COMMENT '单位',
        quote_date DATE NOT NULL COMMENT '页面定价日期',
        source_url VARCHAR(512) DEFAULT NULL COMMENT '抓取来源 URL',
        fetched_at TIMESTAMP NULL DEFAULT NULL COMMENT '最近抓取时间',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '首次入库时间',
        UNIQUE KEY uk_slrp_quote_date (quote_date),
        INDEX idx_slrp_quote_date (quote_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='SMM 1#铅锭公开页参考价历史';
    """,
    # 库房对标城市差额与毛利（配置版），每库房一行
    """
    CREATE TABLE IF NOT EXISTS pd_warehouse_spread_configs (
        id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
        warehouse_id INT NOT NULL COMMENT '库房ID',
        benchmark_city VARCHAR(128) NOT NULL DEFAULT '' COMMENT '对标城市（人工配置）',
        city_spread DECIMAL(18, 4) NOT NULL DEFAULT 0.0000 COMMENT '对标城市差额（可负）',
        gross_margin_config DECIMAL(18, 4) DEFAULT NULL COMMENT '毛利（配置版）',
        warehouse_price DECIMAL(18, 4) DEFAULT NULL COMMENT '库房定价（Excel定价列等人工录入）',
        link_warehouse_id INT DEFAULT NULL COMMENT '用于计算实时价差的绑定对标库房ID',
        link_realtime_spread DECIMAL(18, 4) DEFAULT NULL COMMENT '实时价差=本库房定价-绑定库房定价',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        CONSTRAINT fk_wsc_wh FOREIGN KEY (warehouse_id) REFERENCES dict_warehouses (id)
            ON UPDATE CASCADE ON DELETE CASCADE,
        CONSTRAINT fk_wsc_link_wh FOREIGN KEY (link_warehouse_id) REFERENCES dict_warehouses (id)
            ON UPDATE CASCADE ON DELETE SET NULL,
        UNIQUE KEY uk_wsc_warehouse (warehouse_id),
        INDEX idx_wsc_wh (warehouse_id),
        INDEX idx_wsc_link_wh (link_warehouse_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='库房对标差额与毛利配置';
    """,
    # AI 定价对标分析快照头
    """
    CREATE TABLE IF NOT EXISTS pd_ai_pricing_snapshots (
        id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
        title VARCHAR(255) DEFAULT NULL COMMENT '快照标题',
        as_of_date DATE DEFAULT NULL COMMENT '口径日期（解析基准价/运费的上限日）',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        INDEX idx_aps_created (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='AI定价对标分析快照';
    """,
    # AI 定价对标分析快照明细（落库时固化各字段）
    """
    CREATE TABLE IF NOT EXISTS pd_ai_pricing_snapshot_items (
        id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
        snapshot_id BIGINT NOT NULL COMMENT '快照ID',
        warehouse_id INT NOT NULL COMMENT '库房ID',
        warehouse_name VARCHAR(100) NOT NULL DEFAULT '' COMMENT '库房名称',
        province VARCHAR(64) DEFAULT NULL COMMENT '省',
        city VARCHAR(64) DEFAULT NULL COMMENT '市',
        district VARCHAR(64) DEFAULT NULL COMMENT '区',
        benchmark_city VARCHAR(128) DEFAULT NULL COMMENT '对标城市（配置）',
        benchmark_city_price DECIMAL(18, 4) DEFAULT NULL COMMENT '对标城市定价',
        city_spread DECIMAL(18, 4) DEFAULT NULL COMMENT '对标城市差额',
        gross_margin_config DECIMAL(18, 4) DEFAULT NULL COMMENT '毛利（配置版）',
        calibration_price DECIMAL(18, 4) DEFAULT NULL COMMENT '冶炼厂标定价格（金利）',
        freight DECIMAL(18, 4) DEFAULT NULL COMMENT '库房运费',
        warehouse_price DECIMAL(18, 4) DEFAULT NULL COMMENT '库房定价=对标城市定价+对标城市差额',
        gross_margin_computed DECIMAL(18, 4) DEFAULT NULL COMMENT '毛利（计算版）=标定-运费-库房定价',
        remark TEXT DEFAULT NULL COMMENT '备注',
        CONSTRAINT fk_apsi_snap FOREIGN KEY (snapshot_id) REFERENCES pd_ai_pricing_snapshots (id)
            ON DELETE CASCADE ON UPDATE CASCADE,
        CONSTRAINT fk_apsi_wh FOREIGN KEY (warehouse_id) REFERENCES dict_warehouses (id)
            ON UPDATE CASCADE ON DELETE RESTRICT,
        INDEX idx_apsi_snapshot (snapshot_id),
        INDEX idx_apsi_wh (warehouse_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='AI定价对标分析快照明细';
    """,
    # 垂直库房 AI 定价分析快照（单源库房一次分析一条）
    """
    CREATE TABLE IF NOT EXISTS pd_vertical_warehouse_ai_snapshots (
        id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
        warehouse_id INT NOT NULL COMMENT '源库房ID',
        warehouse_name VARCHAR(100) NOT NULL DEFAULT '' COMMENT '源库房名称',
        title VARCHAR(255) DEFAULT NULL COMMENT '快照标题',
        as_of_date DATE DEFAULT NULL COMMENT '口径日期',
        input_context JSON DEFAULT NULL COMMENT '聚合分析上下文',
        llm_result JSON DEFAULT NULL COMMENT '大模型三方面建议',
        llm_model VARCHAR(128) DEFAULT NULL COMMENT '使用的模型名',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        CONSTRAINT fk_vwais_wh FOREIGN KEY (warehouse_id) REFERENCES dict_warehouses (id)
            ON UPDATE CASCADE ON DELETE RESTRICT,
        INDEX idx_vwais_wh (warehouse_id),
        INDEX idx_vwais_created (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='垂直库房AI定价分析快照';
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


def ensure_pd_ip_prediction_results_analysis_column() -> None:
    """已有库升级：为 pd_ip_prediction_results 补全 analysis（预测依据文案）。"""
    config_dict = get_mysql_config()
    connection = pymysql.connect(**config_dict)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE 'pd_ip_prediction_results'")
            if cursor.fetchone() is None:
                return
            cursor.execute("SHOW COLUMNS FROM pd_ip_prediction_results LIKE 'analysis'")
            if cursor.fetchone() is not None:
                return
            cursor.execute(
                "ALTER TABLE pd_ip_prediction_results "
                "ADD COLUMN analysis TEXT DEFAULT NULL COMMENT '预测依据文案' "
                "AFTER warnings"
            )
            logger.info("已为 pd_ip_prediction_results 添加 analysis 列")
        connection.commit()
    finally:
        connection.close()


def ensure_pd_ip_lead_market_prices_table() -> None:
    """已有库补建：铅价/行情价表（新建库已由 TABLE_STATEMENTS 创建）。"""
    config_dict = get_mysql_config()
    connection = pymysql.connect(**config_dict)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS pd_ip_lead_market_prices (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
                    price_date DATE NOT NULL COMMENT '铅价/行情价日期',
                    lead_price DECIMAL(18, 4) NOT NULL COMMENT '铅价（元/吨，行情基准）',
                    remark VARCHAR(255) DEFAULT NULL COMMENT '备注或数据来源',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                    UNIQUE KEY uk_ip_lead_price_date (price_date),
                    INDEX idx_ip_lead_price_date (price_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='智能预测-铅价/行情价历史';
                """
            )
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


def ensure_pd_ip_delivery_records_enrichment_columns() -> None:
    """已有库升级：送货历史增加地址、中国工作日/节假日标注、天气 JSON。"""
    config_dict = get_mysql_config()
    connection = pymysql.connect(**config_dict)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE 'pd_ip_delivery_records'")
            if cursor.fetchone() is None:
                return

            def _has_col(col: str) -> bool:
                cursor.execute(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema = DATABASE() AND table_name = 'pd_ip_delivery_records' "
                    "AND column_name = %s",
                    (col,),
                )
                return cursor.fetchone()[0] > 0

            if not _has_col("warehouse_address"):
                cursor.execute(
                    "ALTER TABLE pd_ip_delivery_records "
                    "ADD COLUMN warehouse_address VARCHAR(512) DEFAULT NULL COMMENT '仓库地址' AFTER warehouse"
                )
                logger.info("已为 pd_ip_delivery_records 添加 warehouse_address 列")
            if not _has_col("smelter_address"):
                cursor.execute(
                    "ALTER TABLE pd_ip_delivery_records "
                    "ADD COLUMN smelter_address VARCHAR(512) DEFAULT NULL COMMENT '冶炼厂地址' "
                    "AFTER warehouse_address"
                )
                logger.info("已为 pd_ip_delivery_records 添加 smelter_address 列")
            if not _has_col("cn_is_workday"):
                cursor.execute(
                    "ALTER TABLE pd_ip_delivery_records "
                    "ADD COLUMN cn_is_workday TINYINT(1) DEFAULT NULL COMMENT '是否中国工作日(含调休)' AFTER weight"
                )
                logger.info("已为 pd_ip_delivery_records 添加 cn_is_workday 列")
            if not _has_col("cn_calendar_label"):
                cursor.execute(
                    "ALTER TABLE pd_ip_delivery_records "
                    "ADD COLUMN cn_calendar_label VARCHAR(128) DEFAULT NULL COMMENT '导入节假日：仅「是」非工作日或「否」工作日' "
                    "AFTER cn_is_workday"
                )
                logger.info("已为 pd_ip_delivery_records 添加 cn_calendar_label 列")
            if not _has_col("weather_json"):
                cursor.execute(
                    "ALTER TABLE pd_ip_delivery_records "
                    "ADD COLUMN weather_json JSON DEFAULT NULL COMMENT '天气API返回摘要' AFTER cn_calendar_label"
                )
                logger.info("已为 pd_ip_delivery_records 添加 weather_json 列")
            if not _has_col("import_weather"):
                cursor.execute(
                    "ALTER TABLE pd_ip_delivery_records "
                    "ADD COLUMN import_weather VARCHAR(64) DEFAULT NULL COMMENT '导入天气简述，空按晴' AFTER weather_json"
                )
                logger.info("已为 pd_ip_delivery_records 添加 import_weather 列")
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

            # 不再删除 color_config：该列保留为「仓库独立颜色」，与库房类型颜色并存

            try:
                cursor.execute(
                    "ALTER TABLE dict_warehouses ADD INDEX idx_wh_warehouse_type (warehouse_type_id)"
                )
            except Exception:
                pass

        connection.commit()
    finally:
        connection.close()


def ensure_dict_warehouses_color_config_column() -> None:
    """已有库升级：保证 dict_warehouses 存在 color_config（仓库独立颜色）；旧版迁移曾删除该列。"""
    config_dict = get_mysql_config()
    connection = pymysql.connect(**config_dict)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE 'dict_warehouses'")
            if cursor.fetchone() is None:
                return

            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema = DATABASE() AND table_name = 'dict_warehouses' "
                "AND column_name = 'color_config'"
            )
            if cursor.fetchone()[0] > 0:
                return

            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema = DATABASE() AND table_name = 'dict_warehouses' "
                "AND column_name = 'warehouse_type_id'"
            )
            after = "warehouse_type_id" if cursor.fetchone()[0] > 0 else "address"
            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema = DATABASE() AND table_name = 'dict_warehouses' "
                "AND column_name = %s",
                (after,),
            )
            if cursor.fetchone()[0] == 0:
                after = "name"
            cursor.execute(
                f"ALTER TABLE dict_warehouses ADD COLUMN color_config JSON DEFAULT NULL "
                f"COMMENT '仓库独立颜色配置（JSON），可与库房类型颜色并存' AFTER {after}"
            )
            logger.info("已为 dict_warehouses 添加 color_config 列（仓库独立颜色）")
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


def ensure_dict_warehouses_geo_region_columns() -> None:
    """省市区与经纬度（REST 仓库接口与天地图落库）。"""
    config_dict = get_mysql_config()
    connection = pymysql.connect(**config_dict)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE 'dict_warehouses'")
            if cursor.fetchone() is None:
                return

            def _has_col(col: str) -> bool:
                cursor.execute(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema = DATABASE() AND table_name = 'dict_warehouses' "
                    "AND column_name = %s",
                    (col,),
                )
                return cursor.fetchone()[0] > 0

            specs = [
                ("province", "province VARCHAR(64) DEFAULT NULL COMMENT '省'"),
                ("city", "city VARCHAR(64) DEFAULT NULL COMMENT '市'"),
                ("district", "district VARCHAR(64) DEFAULT NULL COMMENT '区县'"),
                (
                    "longitude",
                    "longitude DECIMAL(11, 8) DEFAULT NULL COMMENT '经度'",
                ),
                (
                    "latitude",
                    "latitude DECIMAL(10, 8) DEFAULT NULL COMMENT '纬度'",
                ),
            ]
            for col, frag in specs:
                if not _has_col(col):
                    cursor.execute(f"ALTER TABLE dict_warehouses ADD COLUMN {frag}")
                    logger.info("已为 dict_warehouses 添加列 %s", col)
            try:
                cursor.execute(
                    "CREATE INDEX idx_wh_geo_region ON dict_warehouses "
                    "(province, city, district)"
                )
            except Exception:
                pass
        connection.commit()
    finally:
        connection.close()


def ensure_dict_factories_use_xunrongbao_column() -> None:
    """冶炼厂循融宝发货标记（比价时每吨货物单价加价）。"""
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
                "AND column_name = 'use_xunrongbao'"
            )
            if cursor.fetchone()[0] > 0:
                return
            cursor.execute(
                "ALTER TABLE dict_factories ADD COLUMN use_xunrongbao TINYINT(1) NOT NULL "
                "DEFAULT 0 COMMENT '是否循融宝发货（启用时每吨货物单价加价）' AFTER latitude"
            )
            logger.info("已为 dict_factories 添加 use_xunrongbao 列")
        connection.commit()
    finally:
        connection.close()


def ensure_dict_factories_geo_region_columns() -> None:
    """冶炼厂省市区、颜色、经纬度（与仓库一致，供天地图落库）。"""
    config_dict = get_mysql_config()
    connection = pymysql.connect(**config_dict)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE 'dict_factories'")
            if cursor.fetchone() is None:
                return

            def _has_col(col: str) -> bool:
                cursor.execute(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema = DATABASE() AND table_name = 'dict_factories' "
                    "AND column_name = %s",
                    (col,),
                )
                return cursor.fetchone()[0] > 0

            specs = [
                ("province", "province VARCHAR(64) DEFAULT NULL COMMENT '省'"),
                ("city", "city VARCHAR(64) DEFAULT NULL COMMENT '市'"),
                ("district", "district VARCHAR(64) DEFAULT NULL COMMENT '区县'"),
                (
                    "color_config",
                    "color_config JSON DEFAULT NULL COMMENT '标记颜色等 JSON'",
                ),
                (
                    "longitude",
                    "longitude DECIMAL(11, 8) DEFAULT NULL COMMENT '经度'",
                ),
                (
                    "latitude",
                    "latitude DECIMAL(10, 8) DEFAULT NULL COMMENT '纬度'",
                ),
            ]
            for col, frag in specs:
                if not _has_col(col):
                    cursor.execute(f"ALTER TABLE dict_factories ADD COLUMN {frag}")
                    logger.info("已为 dict_factories 添加列 %s", col)
            try:
                cursor.execute(
                    "CREATE INDEX idx_df_geo_region ON dict_factories "
                    "(province, city, district)"
                )
            except Exception:
                pass
        connection.commit()
    finally:
        connection.close()


def ensure_users_role_varchar_and_role_definitions() -> None:
    """旧库：users.role 从 ENUM 改为 VARCHAR；补全 role_definitions 与角色模板行。"""
    config_dict = get_mysql_config()
    connection = pymysql.connect(**config_dict)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE 'role_definitions'")
            if cursor.fetchone() is None:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS role_definitions (
                        id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
                        code VARCHAR(32) NOT NULL UNIQUE COMMENT '角色代码',
                        name VARCHAR(64) NOT NULL COMMENT '显示名称',
                        description VARCHAR(255) DEFAULT NULL COMMENT '说明',
                        sort_order INT NOT NULL DEFAULT 0 COMMENT '排序',
                        is_system TINYINT(1) NOT NULL DEFAULT 0 COMMENT '1=内置不可删除',
                        is_active TINYINT(1) NOT NULL DEFAULT 1 COMMENT '1=启用',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_role_definitions_active (is_active),
                        INDEX idx_role_definitions_sort (sort_order)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色定义';
                    """
                )
                logger.info("已创建 role_definitions 表")

            cursor.execute("SHOW TABLES LIKE 'users'")
            if cursor.fetchone():
                cursor.execute(
                    """
                    SELECT DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'users'
                    AND COLUMN_NAME = 'role'
                    """
                )
                col = cursor.fetchone()
                if col:
                    ctype = (col[1] or "") + (col[0] or "")
                    if "enum" in ctype.lower():
                        cursor.execute(
                            """
                            ALTER TABLE users MODIFY COLUMN role VARCHAR(32) NOT NULL
                            DEFAULT 'user' COMMENT '角色代码，见 role_definitions.code'
                            """
                        )
                        logger.info("已将 users.role 从 ENUM 迁移为 VARCHAR(32)")

            cursor.execute(
                """
                INSERT IGNORE INTO role_definitions
                (code, name, description, sort_order, is_system, is_active)
                VALUES
                ('admin', '管理员', '系统内置管理员', 0, 1, 1),
                ('user', '普通用户', '系统内置默认角色', 10, 1, 1)
                """
            )

            cursor.execute("SELECT code FROM role_definitions")
            for (code,) in cursor.fetchall():
                cursor.execute(
                    """
                    INSERT IGNORE INTO role_templates (role, template_json)
                    VALUES (%s, '{}')
                    """,
                    (code,),
                )
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


def ensure_pd_pricing_benchmark_tables() -> None:
    """旧库补建：对标定价 / 库房差额 / AI 分析快照等表（新建库已由 TABLE_STATEMENTS 创建）。"""
    config_dict = get_mysql_config()
    connection = pymysql.connect(**config_dict)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS pd_province_benchmark_prices (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
                    province VARCHAR(64) NOT NULL COMMENT '省份',
                    benchmark_city VARCHAR(128) NOT NULL COMMENT '对标城市',
                    benchmark_price DECIMAL(18, 4) NOT NULL COMMENT '对标城市定价',
                    price_date DATE NOT NULL COMMENT '定价日期',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '上传时间',
                    INDEX idx_pbp_province_date (province, price_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='省份对标城市定价历史';
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS pd_smelter_calibration_prices (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
                    factory_id INT NOT NULL COMMENT '冶炼厂ID',
                    calibration_price DECIMAL(18, 4) NOT NULL COMMENT '标定价格',
                    price_date DATE NOT NULL COMMENT '定价日期',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '上传时间',
                    CONSTRAINT fk_scp_factory FOREIGN KEY (factory_id) REFERENCES dict_factories (id)
                        ON UPDATE CASCADE ON DELETE RESTRICT,
                    INDEX idx_scp_factory_date (factory_id, price_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='冶炼厂标定价格历史';
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS pd_smm_lead_reference_prices (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
                    product_name VARCHAR(64) NOT NULL DEFAULT 'SMM 1#铅锭' COMMENT '品种名称',
                    price_low DECIMAL(18, 4) NOT NULL COMMENT '区间最低价',
                    price_high DECIMAL(18, 4) NOT NULL COMMENT '区间最高价',
                    average_price DECIMAL(18, 4) NOT NULL COMMENT '均价=(最低+最高)/2',
                    unit VARCHAR(16) NOT NULL DEFAULT '元/吨' COMMENT '单位',
                    quote_date DATE NOT NULL COMMENT '页面定价日期',
                    source_url VARCHAR(512) DEFAULT NULL COMMENT '抓取来源 URL',
                    fetched_at TIMESTAMP NULL DEFAULT NULL COMMENT '最近抓取时间',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '首次入库时间',
                    UNIQUE KEY uk_slrp_quote_date (quote_date),
                    INDEX idx_slrp_quote_date (quote_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='SMM 1#铅锭公开页参考价历史';
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS pd_warehouse_spread_configs (
                    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
                    warehouse_id INT NOT NULL COMMENT '库房ID',
                    benchmark_city VARCHAR(128) NOT NULL DEFAULT '' COMMENT '对标城市',
                    city_spread DECIMAL(18, 4) NOT NULL DEFAULT 0.0000 COMMENT '对标城市差额',
                    gross_margin_config DECIMAL(18, 4) DEFAULT NULL COMMENT '毛利（配置版）',
                    warehouse_price DECIMAL(18, 4) DEFAULT NULL COMMENT '库房定价',
                    link_warehouse_id INT DEFAULT NULL COMMENT '绑定对标库房ID',
                    link_realtime_spread DECIMAL(18, 4) DEFAULT NULL COMMENT '实时价差',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    CONSTRAINT fk_wsc_wh FOREIGN KEY (warehouse_id) REFERENCES dict_warehouses (id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    UNIQUE KEY uk_wsc_warehouse (warehouse_id),
                    INDEX idx_wsc_wh (warehouse_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='库房对标差额与毛利配置';
                """
            )
            _ensure_pd_warehouse_spread_config_columns(cursor)
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS pd_ai_pricing_snapshots (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
                    title VARCHAR(255) DEFAULT NULL COMMENT '快照标题',
                    as_of_date DATE DEFAULT NULL COMMENT '口径日期',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    INDEX idx_aps_created (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='AI定价对标分析快照';
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS pd_ai_pricing_snapshot_items (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
                    snapshot_id BIGINT NOT NULL COMMENT '快照ID',
                    warehouse_id INT NOT NULL COMMENT '库房ID',
                    warehouse_name VARCHAR(100) NOT NULL DEFAULT '' COMMENT '库房名称',
                    province VARCHAR(64) DEFAULT NULL COMMENT '省',
                    city VARCHAR(64) DEFAULT NULL COMMENT '市',
                    district VARCHAR(64) DEFAULT NULL COMMENT '区',
                    benchmark_city VARCHAR(128) DEFAULT NULL COMMENT '对标城市',
                    benchmark_city_price DECIMAL(18, 4) DEFAULT NULL COMMENT '对标城市定价',
                    city_spread DECIMAL(18, 4) DEFAULT NULL COMMENT '对标城市差额',
                    gross_margin_config DECIMAL(18, 4) DEFAULT NULL COMMENT '毛利（配置版）',
                    calibration_price DECIMAL(18, 4) DEFAULT NULL COMMENT '冶炼厂标定价格',
                    freight DECIMAL(18, 4) DEFAULT NULL COMMENT '库房运费',
                    warehouse_price DECIMAL(18, 4) DEFAULT NULL COMMENT '库房定价',
                    gross_margin_computed DECIMAL(18, 4) DEFAULT NULL COMMENT '毛利（计算版）',
                    remark TEXT DEFAULT NULL COMMENT '备注',
                    CONSTRAINT fk_apsi_snap FOREIGN KEY (snapshot_id) REFERENCES pd_ai_pricing_snapshots (id)
                        ON DELETE CASCADE ON UPDATE CASCADE,
                    CONSTRAINT fk_apsi_wh FOREIGN KEY (warehouse_id) REFERENCES dict_warehouses (id)
                        ON UPDATE CASCADE ON DELETE RESTRICT,
                    INDEX idx_apsi_snapshot (snapshot_id),
                    INDEX idx_apsi_wh (warehouse_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='AI定价对标分析快照明细';
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS pd_vertical_warehouse_ai_snapshots (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
                    warehouse_id INT NOT NULL COMMENT '源库房ID',
                    warehouse_name VARCHAR(100) NOT NULL DEFAULT '' COMMENT '源库房名称',
                    title VARCHAR(255) DEFAULT NULL COMMENT '快照标题',
                    as_of_date DATE DEFAULT NULL COMMENT '口径日期',
                    input_context JSON DEFAULT NULL COMMENT '聚合分析上下文',
                    llm_result JSON DEFAULT NULL COMMENT '大模型三方面建议',
                    llm_model VARCHAR(128) DEFAULT NULL COMMENT '使用的模型名',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    CONSTRAINT fk_vwais_wh FOREIGN KEY (warehouse_id) REFERENCES dict_warehouses (id)
                        ON UPDATE CASCADE ON DELETE RESTRICT,
                    INDEX idx_vwais_wh (warehouse_id),
                    INDEX idx_vwais_created (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='垂直库房AI定价分析快照';
                """
            )
        connection.commit()
        logger.info("pd_* 对标定价相关表已就绪")
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
        ensure_users_role_varchar_and_role_definitions()
    except Exception:
        logger.exception("检查/迁移 users.role 与 role_definitions 失败")
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
        ensure_pd_ip_delivery_records_enrichment_columns()
    except Exception:
        logger.exception("检查/添加 pd_ip_delivery_records 地址/节假日/天气列失败")
    try:
        ensure_pd_ip_prediction_results_smelter_column()
    except Exception:
        logger.exception("检查/添加 pd_ip_prediction_results.smelter 失败")
    try:
        ensure_pd_ip_prediction_results_analysis_column()
    except Exception:
        logger.exception("检查/添加 pd_ip_prediction_results.analysis 失败")
    try:
        ensure_pd_ip_lead_market_prices_table()
    except Exception:
        logger.exception("检查/创建 pd_ip_lead_market_prices 失败")
    try:
        ensure_dict_warehouses_extended_columns()
    except Exception:
        logger.exception("检查/添加 dict_warehouses 扩展列失败")
    try:
        ensure_dict_warehouse_types_migration()
    except Exception:
        logger.exception("库房类型表/warehouse_type_id 迁移失败")
    try:
        ensure_dict_warehouses_color_config_column()
    except Exception:
        logger.exception("检查/添加 dict_warehouses.color_config（仓库颜色）失败")
    try:
        ensure_dict_factories_address_column()
    except Exception:
        logger.exception("检查/添加 dict_factories.address 失败")
    try:
        ensure_dict_warehouses_geo_region_columns()
    except Exception:
        logger.exception("检查/添加 dict_warehouses 省市区与经纬度失败")
    try:
        ensure_dict_factories_geo_region_columns()
    except Exception:
        logger.exception("检查/添加 dict_factories 省市区与经纬度失败")
    try:
        ensure_dict_factories_use_xunrongbao_column()
    except Exception:
        logger.exception("检查/添加 dict_factories.use_xunrongbao 失败")
    try:
        ensure_dict_warehouse_links_table()
    except Exception:
        logger.exception("检查/创建 dict_warehouse_links 库房关联边表失败")
    try:
        ensure_dict_warehouses_business_columns()
    except Exception:
        logger.exception("检查/添加 dict_warehouses 业务扩展列失败")
    try:
        ensure_dict_warehouses_inventory_price_columns()
    except Exception:
        logger.exception("检查/添加 dict_warehouses 当前库存/收货价格列失败")
    try:
        ensure_dict_warehouse_links_tier_price_spread_column()
    except Exception:
        logger.exception("检查/添加 dict_warehouse_links.tier_price_spread 失败")
    try:
        ensure_pd_pricing_benchmark_tables()
    except Exception:
        logger.exception("检查/创建 pd_* 对标定价相关表失败")
    try:
        ensure_pd_warehouse_spread_config_price_columns()
    except Exception:
        logger.exception("检查/添加 pd_warehouse_spread_configs 库房定价与实时价差列失败")
    try:
        ensure_pd_vertical_warehouse_ai_snapshots_table()
    except Exception:
        logger.exception("检查/创建 pd_vertical_warehouse_ai_snapshots 表失败")
    try:
        ensure_warehouse_inventory_and_receipt_price_tables()
    except Exception:
        logger.exception("检查/创建库房库存快照与按品种收货价格表失败")


def ensure_warehouse_inventory_and_receipt_price_tables() -> None:
    """旧库补建：库房库存快照、按品种收货价格表。"""
    config_dict = get_mysql_config()
    connection = pymysql.connect(**config_dict)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS warehouse_inventory_snapshots (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
                    warehouse_id INT NOT NULL COMMENT '库房ID',
                    category_id INT NOT NULL COMMENT '品类分组ID（dict_categories.category_id）',
                    inventory_ton DECIMAL(14, 4) NOT NULL COMMENT '当前库存(吨)',
                    inventory_date DATE NOT NULL COMMENT '库存日期',
                    source VARCHAR(32) NOT NULL DEFAULT 'import' COMMENT '来源：import/manual',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    CONSTRAINT fk_wis_warehouse FOREIGN KEY (warehouse_id) REFERENCES dict_warehouses (id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    UNIQUE KEY uk_wis_wh_cat_date (warehouse_id, category_id, inventory_date),
                    INDEX idx_wis_wh_cat_date (warehouse_id, category_id, inventory_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='库房库存快照（按库房、品类、日期）';
                """
            )
            cursor.execute("SHOW TABLES LIKE 'warehouse_inventory_snapshots'")
            if cursor.fetchone():

                def _wis_has_col(col: str) -> bool:
                    cursor.execute(
                        "SELECT COUNT(*) FROM information_schema.columns "
                        "WHERE table_schema = DATABASE() "
                        "AND table_name = 'warehouse_inventory_snapshots' "
                        "AND column_name = %s",
                        (col,),
                    )
                    return cursor.fetchone()[0] > 0

                def _wis_has_index(name: str) -> bool:
                    cursor.execute(
                        "SELECT COUNT(*) FROM information_schema.statistics "
                        "WHERE table_schema = DATABASE() "
                        "AND table_name = 'warehouse_inventory_snapshots' "
                        "AND index_name = %s",
                        (name,),
                    )
                    return cursor.fetchone()[0] > 0

                needs_category_col = not _wis_has_col("category_id")
                needs_new_indexes = (
                    not _wis_has_index("uk_wis_wh_cat_date")
                    or not _wis_has_index("idx_wis_wh_cat_date")
                )
                has_old_indexes = _wis_has_index("uk_wis_wh_date") or _wis_has_index(
                    "idx_wis_wh_date"
                )
                if needs_category_col or needs_new_indexes or has_old_indexes:
                    if needs_category_col:
                        cursor.execute("DELETE FROM warehouse_inventory_snapshots")
                        cursor.execute(
                            "ALTER TABLE warehouse_inventory_snapshots "
                            "ADD COLUMN category_id INT NOT NULL COMMENT "
                            "'品类分组ID（dict_categories.category_id）' "
                            "AFTER warehouse_id"
                        )
                    # 先建含 warehouse_id 前缀的新索引，再删旧索引（外键 fk_wis_warehouse 依赖该列索引）
                    if not _wis_has_index("idx_wis_wh_cat_date"):
                        cursor.execute(
                            "ALTER TABLE warehouse_inventory_snapshots "
                            "ADD INDEX idx_wis_wh_cat_date "
                            "(warehouse_id, category_id, inventory_date)"
                        )
                    if not _wis_has_index("uk_wis_wh_cat_date"):
                        cursor.execute(
                            "ALTER TABLE warehouse_inventory_snapshots "
                            "ADD UNIQUE KEY uk_wis_wh_cat_date "
                            "(warehouse_id, category_id, inventory_date)"
                        )
                    if _wis_has_index("uk_wis_wh_date"):
                        cursor.execute(
                            "ALTER TABLE warehouse_inventory_snapshots "
                            "DROP INDEX uk_wis_wh_date"
                        )
                    if _wis_has_index("idx_wis_wh_date"):
                        cursor.execute(
                            "ALTER TABLE warehouse_inventory_snapshots "
                            "DROP INDEX idx_wis_wh_date"
                        )
                    logger.info(
                        "已为 warehouse_inventory_snapshots 完成 category_id/索引迁移"
                    )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS warehouse_category_receipt_prices (
                    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
                    warehouse_id INT NOT NULL COMMENT '库房ID',
                    category_id INT NOT NULL COMMENT '品类分组ID（dict_categories.category_id）',
                    price_per_ton DECIMAL(14, 4) NOT NULL COMMENT '库房回收单价(元/吨)',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    CONSTRAINT fk_wcrp_warehouse FOREIGN KEY (warehouse_id) REFERENCES dict_warehouses (id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    UNIQUE KEY uk_wcrp_wh_cat (warehouse_id, category_id),
                    INDEX idx_wcrp_wh (warehouse_id),
                    INDEX idx_wcrp_cat (category_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='库房按品种收货价格';
                """
            )
        connection.commit()
    finally:
        connection.close()


def ensure_pd_vertical_warehouse_ai_snapshots_table() -> None:
    """旧库补建垂直库房 AI 分析快照表。"""
    config_dict = get_mysql_config()
    connection = pymysql.connect(**config_dict)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS pd_vertical_warehouse_ai_snapshots (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
                    warehouse_id INT NOT NULL COMMENT '源库房ID',
                    warehouse_name VARCHAR(100) NOT NULL DEFAULT '' COMMENT '源库房名称',
                    title VARCHAR(255) DEFAULT NULL COMMENT '快照标题',
                    as_of_date DATE DEFAULT NULL COMMENT '口径日期',
                    input_context JSON DEFAULT NULL COMMENT '聚合分析上下文',
                    llm_result JSON DEFAULT NULL COMMENT '大模型三方面建议',
                    llm_model VARCHAR(128) DEFAULT NULL COMMENT '使用的模型名',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    CONSTRAINT fk_vwais_wh FOREIGN KEY (warehouse_id) REFERENCES dict_warehouses (id)
                        ON UPDATE CASCADE ON DELETE RESTRICT,
                    INDEX idx_vwais_wh (warehouse_id),
                    INDEX idx_vwais_created (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='垂直库房AI定价分析快照';
                """
            )
        connection.commit()
    finally:
        connection.close()


def _ensure_pd_warehouse_spread_config_columns(cursor) -> None:
    """旧库补列：库房定价、绑定库房、实时价差（CREATE IF NOT EXISTS 不含新列时）。"""
    cursor.execute("SHOW TABLES LIKE 'pd_warehouse_spread_configs'")
    if cursor.fetchone() is None:
        return
    alters = [
        (
            "warehouse_price",
            "ALTER TABLE pd_warehouse_spread_configs ADD COLUMN warehouse_price "
            "DECIMAL(18, 4) DEFAULT NULL COMMENT '库房定价（Excel定价列等）' AFTER gross_margin_config",
        ),
        (
            "link_warehouse_id",
            "ALTER TABLE pd_warehouse_spread_configs ADD COLUMN link_warehouse_id INT DEFAULT NULL "
            "COMMENT '绑定对标库房ID' AFTER warehouse_price",
        ),
        (
            "link_realtime_spread",
            "ALTER TABLE pd_warehouse_spread_configs ADD COLUMN link_realtime_spread "
            "DECIMAL(18, 4) DEFAULT NULL COMMENT '实时价差=本库房定价-绑定库房定价' AFTER link_warehouse_id",
        ),
    ]
    for col, ddl in alters:
        cursor.execute(
            "SELECT COUNT(*) FROM information_schema.COLUMNS "
            "WHERE table_schema = DATABASE() AND table_name = 'pd_warehouse_spread_configs' "
            "AND column_name = %s",
            (col,),
        )
        if cursor.fetchone()[0] == 0:
            cursor.execute(ddl)
            logger.info("已为 pd_warehouse_spread_configs 添加 %s 列", col)


def ensure_pd_warehouse_spread_config_price_columns() -> None:
    config_dict = get_mysql_config()
    connection = pymysql.connect(**config_dict)
    try:
        with connection.cursor() as cursor:
            _ensure_pd_warehouse_spread_config_columns(cursor)
        connection.commit()
    finally:
        connection.close()


def ensure_dict_warehouses_business_columns() -> None:
    """联系人、电话、危废许可量、月均收货、参考运费（旧库补列）。"""
    config_dict = get_mysql_config()
    connection = pymysql.connect(**config_dict)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE 'dict_warehouses'")
            if cursor.fetchone() is None:
                return

            def _has_col(col: str) -> bool:
                cursor.execute(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema = DATABASE() AND table_name = 'dict_warehouses' "
                    "AND column_name = %s",
                    (col,),
                )
                return cursor.fetchone()[0] > 0

            if not _has_col("contact_name"):
                cursor.execute(
                    "ALTER TABLE dict_warehouses ADD COLUMN contact_name VARCHAR(64) DEFAULT NULL "
                    "COMMENT '库房联系人' AFTER latitude"
                )
                logger.info("已为 dict_warehouses 添加 contact_name")
            if not _has_col("contact_phone"):
                cursor.execute(
                    "ALTER TABLE dict_warehouses ADD COLUMN contact_phone VARCHAR(32) DEFAULT NULL "
                    "COMMENT '电话' AFTER contact_name"
                )
                logger.info("已为 dict_warehouses 添加 contact_phone")
            if not _has_col("hazardous_waste_license_qty"):
                cursor.execute(
                    "ALTER TABLE dict_warehouses ADD COLUMN hazardous_waste_license_qty "
                    "DECIMAL(14, 4) DEFAULT NULL COMMENT '危废经营许可数量' AFTER contact_phone"
                )
                logger.info("已为 dict_warehouses 添加 hazardous_waste_license_qty")
            if not _has_col("monthly_avg_receipt_ton"):
                cursor.execute(
                    "ALTER TABLE dict_warehouses ADD COLUMN monthly_avg_receipt_ton "
                    "DECIMAL(14, 4) DEFAULT NULL COMMENT '月均收货(吨)' AFTER hazardous_waste_license_qty"
                )
                logger.info("已为 dict_warehouses 添加 monthly_avg_receipt_ton")
            if not _has_col("freight_amount"):
                cursor.execute(
                    "ALTER TABLE dict_warehouses ADD COLUMN freight_amount DECIMAL(14, 4) DEFAULT NULL "
                    "COMMENT '运费参考(元)' AFTER monthly_avg_receipt_ton"
                )
                logger.info("已为 dict_warehouses 添加 freight_amount")
        connection.commit()
    finally:
        connection.close()


def ensure_dict_warehouses_inventory_price_columns() -> None:
    """当前库存、收货价格（旧库补列）。"""
    config_dict = get_mysql_config()
    connection = pymysql.connect(**config_dict)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE 'dict_warehouses'")
            if cursor.fetchone() is None:
                return

            def _has_col(col: str) -> bool:
                cursor.execute(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema = DATABASE() AND table_name = 'dict_warehouses' "
                    "AND column_name = %s",
                    (col,),
                )
                return cursor.fetchone()[0] > 0

            if not _has_col("current_inventory_ton"):
                cursor.execute(
                    "ALTER TABLE dict_warehouses ADD COLUMN current_inventory_ton "
                    "DECIMAL(14, 4) DEFAULT NULL COMMENT '当前库存(吨)' "
                    "AFTER monthly_avg_receipt_ton"
                )
                logger.info("已为 dict_warehouses 添加 current_inventory_ton")
            if not _has_col("receipt_price_per_ton"):
                cursor.execute(
                    "ALTER TABLE dict_warehouses ADD COLUMN receipt_price_per_ton "
                    "DECIMAL(14, 4) DEFAULT NULL COMMENT '收货价格(元/吨)' "
                    "AFTER current_inventory_ton"
                )
                logger.info("已为 dict_warehouses 添加 receipt_price_per_ton")
        connection.commit()
    finally:
        connection.close()


def ensure_dict_warehouse_links_tier_price_spread_column() -> None:
    """库房关联边阶梯价差 JSON。"""
    config_dict = get_mysql_config()
    connection = pymysql.connect(**config_dict)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE 'dict_warehouse_links'")
            if cursor.fetchone() is None:
                return

            def _has_col(col: str) -> bool:
                cursor.execute(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema = DATABASE() AND table_name = 'dict_warehouse_links' "
                    "AND column_name = %s",
                    (col,),
                )
                return cursor.fetchone()[0] > 0

            if not _has_col("tier_price_spread"):
                cursor.execute(
                    "ALTER TABLE dict_warehouse_links ADD COLUMN tier_price_spread JSON DEFAULT NULL "
                    "COMMENT '阶梯价差（JSON）' AFTER to_warehouse_id"
                )
                logger.info("已为 dict_warehouse_links 添加 tier_price_spread")
        connection.commit()
    finally:
        connection.close()


def ensure_dict_warehouse_links_table() -> None:
    """库房单向关联边表（新库由 TABLE_STATEMENTS 创建；旧库补建）。"""
    config_dict = get_mysql_config()
    connection = pymysql.connect(**config_dict)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS dict_warehouse_links (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '边ID',
                    from_warehouse_id INT NOT NULL COMMENT '源库房（出边起点）',
                    to_warehouse_id INT NOT NULL COMMENT '对标库房（单向指向终点）',
                    tier_price_spread JSON DEFAULT NULL COMMENT '阶梯价差（JSON）',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    UNIQUE KEY uk_wh_link_from_to (from_warehouse_id, to_warehouse_id),
                    INDEX idx_wh_link_from (from_warehouse_id),
                    INDEX idx_wh_link_to (to_warehouse_id),
                    CONSTRAINT fk_wh_link_from FOREIGN KEY (from_warehouse_id)
                        REFERENCES dict_warehouses (id) ON DELETE CASCADE ON UPDATE CASCADE,
                    CONSTRAINT fk_wh_link_to FOREIGN KEY (to_warehouse_id)
                        REFERENCES dict_warehouses (id) ON DELETE CASCADE ON UPDATE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='库房单向关联（有向图边）';
                """
            )
        connection.commit()
        logger.info("dict_warehouse_links 表已就绪")
    finally:
        connection.close()


if __name__ == "__main__":
    create_tables()

from contextlib import contextmanager

import pymysql

from app import config


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
            print(f"数据库 '{config.MYSQL_DATABASE}' 检查/创建完成")
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
    # 仓库字典表
    """
    CREATE TABLE IF NOT EXISTS dict_warehouses (
        id INT AUTO_INCREMENT PRIMARY KEY COMMENT '仓库ID',
        name VARCHAR(100) NOT NULL UNIQUE COMMENT '仓库名称',
        is_active TINYINT(1) DEFAULT 1 COMMENT '是否启用',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='仓库字典表';
    """,
    # 冶炼厂字典表
    """
    CREATE TABLE IF NOT EXISTS dict_factories (
        id INT AUTO_INCREMENT PRIMARY KEY COMMENT '冶炼厂ID',
        name VARCHAR(100) NOT NULL UNIQUE COMMENT '冶炼厂名称',
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
        category_id INT NOT NULL COMMENT '品类行ID（关联dict_categories.row_id）',
        metadata_id INT COMMENT '关联报价表元数据ID',
        raw_category_name VARCHAR(100) COMMENT '原始品类名',
        unit_price DECIMAL(10, 2) COMMENT '普通单价（元/吨）',
        price_1pct_vat DECIMAL(10, 2) COMMENT '1%增值税价格',
        price_3pct_vat DECIMAL(10, 2) COMMENT '3%增值税价格',
        price_13pct_vat DECIMAL(10, 2) COMMENT '13%增值税价格',
        price_normal_invoice DECIMAL(10, 2) COMMENT '普通发票价格',
        price_reverse_invoice DECIMAL(10, 2) COMMENT '反向发票价格',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        CONSTRAINT fk_detail_factory FOREIGN KEY (factory_id) REFERENCES dict_factories (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        CONSTRAINT fk_detail_category FOREIGN KEY (category_id) REFERENCES dict_categories (row_id) ON UPDATE CASCADE ON DELETE RESTRICT,
        CONSTRAINT fk_detail_metadata FOREIGN KEY (metadata_id) REFERENCES quote_table_metadata (id) ON UPDATE CASCADE ON DELETE SET NULL,
        UNIQUE KEY uk_factory_category_date (factory_id, category_id, quote_date),
        INDEX idx_quote_date (quote_date),
        INDEX idx_factory_id (factory_id)
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
]


def create_tables() -> None:
    create_database_if_not_exists()
    config_dict = get_mysql_config()
    connection = pymysql.connect(**config_dict)
    try:
        with connection.cursor() as cursor:
            for statement in TABLE_STATEMENTS:
                cursor.execute(statement)
        connection.commit()
        print("所有数据表创建完成")
    finally:
        connection.close()


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
        print("默认数据初始化完成")
    finally:
        connection.close()


if __name__ == "__main__":
    create_tables()

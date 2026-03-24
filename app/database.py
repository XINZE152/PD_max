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
    # 品类字典表
    """
    CREATE TABLE IF NOT EXISTS dict_categories (
        row_id INT AUTO_INCREMENT PRIMARY KEY COMMENT '行主键（自增，唯一）',
        category_id INT NOT NULL COMMENT '品类分组ID（多名称共用同一值，如铜=301）',
        category_code VARCHAR(20) NOT NULL UNIQUE COMMENT '品类业务码（如：CAT_CU），不随名称变化',
        name VARCHAR(50) NOT NULL UNIQUE COMMENT '品类名称（如：紫铜、黄铜）',
        is_main TINYINT(1) DEFAULT 0 COMMENT '是否主品类：1-是（用于比价表展示），0-否',
        is_active TINYINT(1) DEFAULT 1 COMMENT '是否启用',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_category_id (category_id),
        INDEX idx_category_main (category_id, is_main),
        INDEX idx_is_main (is_main)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='品类字典表（多名称共用同一category_id）';
    """,
    # 仓库字典表
    """
    CREATE TABLE IF NOT EXISTS dict_warehouses (
        id INT AUTO_INCREMENT PRIMARY KEY COMMENT '行主键（自增，唯一）',
        warehouse_code VARCHAR(20) NOT NULL UNIQUE COMMENT '仓库业务码（如：WH_SH），不随名称变化',
        name VARCHAR(100) NOT NULL UNIQUE COMMENT '仓库名称',
        location VARCHAR(100) COMMENT '仓库地址',
        is_active TINYINT(1) DEFAULT 1 COMMENT '是否启用',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='仓库字典表';
    """,
    # 冶炼厂字典表
    """
    CREATE TABLE IF NOT EXISTS dict_factories (
        id INT AUTO_INCREMENT PRIMARY KEY COMMENT '行主键（自增，唯一）',
        factory_code VARCHAR(20) NOT NULL UNIQUE COMMENT '冶炼厂业务码（如：FAC_BJ），不随名称变化',
        name VARCHAR(100) NOT NULL UNIQUE COMMENT '冶炼厂名称',
        location VARCHAR(100) COMMENT '地点',
        contact VARCHAR(50) COMMENT '联系人',
        phone VARCHAR(30) COMMENT '联系电话',
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
    # 报价主单表
    """
    CREATE TABLE IF NOT EXISTS quote_orders (
        id INT AUTO_INCREMENT PRIMARY KEY,
        quote_date DATE NOT NULL COMMENT '报价日期',
        upload_batch_no VARCHAR(40) COMMENT '上传批次号（同一天可多批）',
        supplier_id INT COMMENT '供应商ID（关联dict_factories.id）',
        status ENUM('DRAFT', 'CONFIRMED', 'CLOSED') DEFAULT 'DRAFT' COMMENT '状态',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        CONSTRAINT fk_order_supplier FOREIGN KEY (supplier_id) REFERENCES dict_factories (id) ON UPDATE CASCADE ON DELETE SET NULL,
        INDEX idx_quote_date (quote_date),
        INDEX idx_quote_batch (quote_date, upload_batch_no)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='报价主单表';
    """,
    # 报价明细表
    """
    CREATE TABLE IF NOT EXISTS quote_details (
        id INT AUTO_INCREMENT PRIMARY KEY,
        order_id INT NOT NULL COMMENT '主单ID',
        factory_id INT NOT NULL COMMENT '冶炼厂ID',
        raw_category_name VARCHAR(100) NOT NULL COMMENT '报价原始品类名（如：电动车电池）',
        mapped_category_row_id INT COMMENT '映射到dict_categories.row_id（具体别名行）',
        category_id INT COMMENT '归并后大类ID（如：电池=301，用于利润聚合）',
        weight_tons DECIMAL(10, 2) NOT NULL COMMENT '重量（吨）',
        unit_price DECIMAL(10, 2) NOT NULL COMMENT '单价（元/吨）',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        CONSTRAINT fk_detail_order FOREIGN KEY (order_id) REFERENCES quote_orders (id) ON DELETE CASCADE,
        CONSTRAINT fk_detail_factory FOREIGN KEY (factory_id) REFERENCES dict_factories (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        CONSTRAINT fk_detail_category_row FOREIGN KEY (mapped_category_row_id) REFERENCES dict_categories (row_id) ON UPDATE CASCADE ON DELETE SET NULL,
        INDEX idx_order_id (order_id),
        INDEX idx_factory_id (factory_id),
        INDEX idx_mapped_category_row (mapped_category_row_id),
        INDEX idx_category_id (category_id),
        INDEX idx_raw_category_name (raw_category_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='报价明细表';
    """,
    # 利润计算结果表
    """
    CREATE TABLE IF NOT EXISTS optimization_results (
        id INT AUTO_INCREMENT PRIMARY KEY,
        order_id INT NOT NULL UNIQUE COMMENT '主单ID',
        total_profit DECIMAL(15, 2) DEFAULT 0.00 COMMENT '总利润（元）',
        best_combination JSON COMMENT '最优组合方案',
        calculation_time_ms INT COMMENT '计算耗时（毫秒）',
        error_msg TEXT COMMENT '错误信息',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        CONSTRAINT fk_result_order FOREIGN KEY (order_id) REFERENCES quote_orders (id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='利润计算结果表';
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


if __name__ == "__main__":
    create_tables()

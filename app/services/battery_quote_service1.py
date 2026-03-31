import os
import re
import logging
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from typing import List, Dict, Any, Optional, Tuple
from decimal import Decimal




# 假设 RapidOCR 已安装
try:
    from rapidocr_onnxruntime import RapidOCR
except ImportError:
    # 如果没有安装，提供一个 mock 类防止报错，实际运行请确保安装：pip install rapidocr_onnxruntime
    class RapidOCR:
        def __call__(self, img):
            raise ImportError("请先安装 rapidocr_onnxruntime: pip install rapidocr_onnxruntime")

logger = logging.getLogger(__name__)

class BatteryQuoteItem:
    def __init__(self, category: str, price: Decimal):
        self.category = category
        self.price = price

    def to_dict(self) -> Dict[str, Any]:
        return {
            "category": self.category,
            "price": float(self.price)
        }

class BatteryQuoteService:
    def __init__(self):
        logger.info("RapidOCR 初始化成功")
        self.ocr = RapidOCR()

    def _parse(self, ocr_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        解析调度中心：
        1. 提取文本行和全文本
        2. 根据特征判断类型
        3. 分发到具体的解析器 (_parse_table_style 或 _parse_chat_style)
        """
        # 确保 OCR 结果格式正确
        if not ocr_result or 'lines' not in ocr_result:
            return {"error": "OCR 结果为空或格式错误", "items": []}

        text_lines = ocr_result.get("lines", [])
        full_text = "\n".join([line.get("text", "") for line in text_lines])

        # === 策略 1: 优先尝试表格解析 (增强版) ===
        if len(text_lines) > 5:
            logger.info("🔍 检测到多行内容，尝试【增强版表格】解析...")
            table_result = self._parse_table_style(text_lines, full_text)

            if table_result.get("items") and len(table_result["items"]) > 0:
                logger.info(f"✅ 表格解析成功！共提取 {len(table_result['items'])} 条数据")
                return table_result
            else:
                logger.warning("⚠️ 表格解析未提取到有效数据，准备降级...")

        # === 策略 2: 降级为聊天截图解析 ===
        logger.info("🔍 尝试【聊天截图】解析...")
        chat_result = self._parse_chat_style(text_lines, full_text)

        if chat_result.get("items"):
            logger.info(f"✅ 聊天截图解析成功！共提取 {len(chat_result['items'])} 条数据")
        else:
            logger.warning("⚠️ 所有解析策略均未提取到数据。")

        return chat_result



    def _to_date(self, value: Any) -> Optional[date]:
        """统一日期转换工具 - 支持多种格式及无年份日期"""
        if value in (None, ""):
            return None
        if isinstance(value, date):
            return value

        val_str = str(value).strip()

        # 1. 尝试提取纯 8 位数字 (YYYYMMDD)
        clean_value = re.sub(r"[^0-9]", "", val_str)
        try:
            if len(clean_value) == 8:
                return datetime.strptime(clean_value, "%Y%m%d").date()
        except Exception:
            pass

        # 2. 尝试标准日期格式 (带分隔符)
        for fmt in ["%Y-%m-%d", "%Y年%m月%d日", "%Y.%m.%d", "%Y/%m/%d"]:
            try:
                temp_val = val_str.replace("年", "-").replace("月", "-").replace("日", "").replace("号", "")
                return datetime.strptime(temp_val, fmt).date()
            except ValueError:
                continue

        # 3. 处理 "M 月 D 号" 或 "M 月 D 日" (无年份) 的情况
        match_md = re.search(r"(\d{1,2})[月](\d{1,2})[号日]?", val_str)
        if match_md:
            try:
                month = int(match_md.group(1))
                day = int(match_md.group(2))
                year = datetime.now().year  # 默认使用当前年份

                if 1 <= month <= 12 and 1 <= day <= 31:
                    return date(year, month, day)
            except Exception:
                pass

        return None

    def _is_same_line(self, box1: List[int], box2: List[int], threshold=15) -> bool:
        """判断两个文本框是否在同一行 (基于 Y 轴中心点)"""
        if not box1 or not box2:
            return False
        y1_center = (box1[1] + box1[3]) / 2
        y2_center = (box2[1] + box2[3]) / 2
        return abs(y1_center - y2_center) < threshold

    def _parse_table_by_coords(self, text_lines: List[Dict], full_text: str) -> Optional[Dict[str, Any]]:
        """
        【核心功能】基于坐标布局分析的表格解析器
        原理：通过 X 轴投影找到"品类"和"价格"两列，再按 Y 轴分组配对。
        """
        if not text_lines or len(text_lines) < 3:
            return None

        # 1. 寻找表头关键词，确定数据起始行
        header_keywords = ["种类", "品类", "名称", "项目", "型号"]
        header_idx = -1

        for i, line in enumerate(text_lines):
            txt = line.get("text", "")
            if any(k in txt for k in header_keywords):
                header_idx = i
                break

        data_start_idx = header_idx + 1 if header_idx != -1 else 0

        potential_data_lines = []
        for i, line in enumerate(text_lines):
            if i < data_start_idx:
                continue
            txt = line.get("text", "")
            bbox = line.get("bbox")
            if bbox and re.search(r"\d{4,}", txt):
                potential_data_lines.append(line)

        if len(potential_data_lines) < 2:
            return None

        # 2. 分析 X 轴分布，寻找列分割点
        centers_x = []
        for line in potential_data_lines:
            bbox = line.get("bbox")
            if bbox:
                cx = (bbox[0] + bbox[2]) / 2
                centers_x.append(cx)

        if not centers_x:
            return None

        centers_x.sort()

        if len(centers_x) >= 6:
            clusters = []
            for cx in centers_x:
                found = False
                for cluster in clusters:
                    if abs(cx - cluster['center']) < 50:
                        cluster['count'] += 1
                        cluster['center'] = (cluster['center'] * (cluster['count'] - 1) + cx) / cluster['count']
                        found = True
                        break
                if not found:
                    clusters.append({'center': cx, 'count': 1})

            if len(clusters) > 2:
                logger.info(f"检测到 {len(clusters)} 列表格，坐标法不适用，改用其他解析器")
                return None

        max_gap = 0
        split_x = 0
        for i in range(len(centers_x) - 1):
            gap = centers_x[i+1] - centers_x[i]
            if gap > max_gap:
                max_gap = gap
                split_x = (centers_x[i] + centers_x[i+1]) / 2

        if max_gap < 50:
            return None

        # 3. 按 Y 轴分组
        rows = []
        current_row = []
        last_y_center = -1000

        sorted_lines = sorted(potential_data_lines, key=lambda x: (x.get("bbox", [0,0,0,0])[1] + x.get("bbox", [0,0,0,0])[3])/2)

        for line in sorted_lines:
            bbox = line.get("bbox")
            if not bbox: continue

            y_center = (bbox[1] + bbox[3]) / 2

            if abs(y_center - last_y_center) > 20:
                if current_row:
                    rows.append(current_row)
                current_row = [line]
                last_y_center = y_center
            else:
                current_row.append(line)
        if current_row:
            rows.append(current_row)

        # 4. 提取每行的品类和价格
        items = []
        for row in rows:
            cat_parts = []
            price_parts = []

            for item in row:
                bbox = item.get("bbox")
                txt = item.get("text", "").strip()
                if not txt: continue

                cx = (bbox[0] + bbox[2]) / 2

                if cx < split_x:
                    cat_parts.append(txt)
                else:
                    price_parts.append(txt)

            category = "".join(cat_parts).strip()
            price_raw = "".join(price_parts).strip()

            if any(k in category for k in ["种类", "价格", "备注", "序号"]):
                continue

            price_match = re.search(r"(\d{4,})", price_raw)
            if price_match and category:
                try:
                    price_val = Decimal(price_match.group(1))
                    if 1000 < price_val < 20000:
                        items.append(BatteryQuoteItem(category=category, price=price_val))
                except InvalidOperation:
                    continue

        if not items:
            return None

        # 5. 提取工厂和日期
        factory_name = "未知工厂"
        match_factory = re.search(r"([\u4e00-\u9fa5]{4,}有限公司)", full_text)
        if match_factory:
            factory_name = match_factory.group(1)

        exec_date = None
        d_match = re.search(r"\((\d{8})\)", full_text)
        if d_match:
            exec_date = self._to_date(d_match.group(1))
        else:
            md_match = re.search(r"(\d{1,2})[月](\d{1,2})[号日]", full_text)
            if md_match:
                exec_date = self._to_date(f"{datetime.now().year}年{md_match.group(1)}月{md_match.group(2)}日")
            else:
                std_match = re.search(r"(\d{4}[-./年]\d{1,2}[-./月]\d{1,2}[日号]?)", full_text)
                if std_match:
                    exec_date = self._to_date(std_match.group(1))

        return {
            "factory_name": factory_name,
            "exec_date": exec_date.strftime("%Y-%m-%d") if exec_date else None,
            "quote_type": "table_coord",
            "items": [item.to_dict() for item in items],
            "raw_text": full_text
        }

    def _parse_table_style(self, text_lines: List[Dict], full_text: str) -> Dict[str, Any]:
        """
        【终极版】支持长品类名称 + 复杂日期格式
        """
        factory_name = "未知工厂"
        exec_date = None

        # --- 1. 工厂提取 (增强版：处理跨行公司名) ---
        match_factory = re.search(r"([\u4e00-\u9fa5]{4,}有限公司)", full_text)
        if match_factory:
            factory_name = match_factory.group(1)
        else:
            match_partial = re.search(r"([\u4e00-\u9fa5]{4,}有限公?司?)", full_text)
            if match_partial:
                partial_name = match_partial.group(1)
                if partial_name.endswith("有限公"):
                    factory_name = partial_name + "司"
                else:
                    factory_name = partial_name
            else:
                keywords = ["鲁控", "亿晨", "骆驼", "天能", "超威", "环保", "科技"]
                for kw in keywords:
                    match = re.search(r"([\u4e00-\u9fa5]*" + kw + r"[\u4e00-\u9fa5]*公司?)", full_text)
                    if match:
                        factory_name = match.group(1)
                        break

        # --- 2. 日期提取 (超级增强版) ---
        date_match = re.search(r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日\.?", full_text)
        if date_match:
            try:
                y, m, d = int(date_match.group(1)), int(date_match.group(2)), int(date_match.group(3))
                exec_date = date(y, m, d)
            except ValueError:
                pass

        if not exec_date:
            bracket_match = re.search(r"[\(（【](\d{4})\s*(\d{2})\s*(\d{2})[\)）】]", full_text)
            if bracket_match:
                try:
                    y, m, d = int(bracket_match.group(1)), int(bracket_match.group(2)), int(bracket_match.group(3))
                    exec_date = date(y, m, d)
                except ValueError:
                    pass

        if not exec_date:
            md_match = re.search(r"(\d{1,2})月\s*(\d{1,2})[号日]", full_text)
            if md_match:
                try:
                    y = datetime.now().year
                    m, d = int(md_match.group(1)), int(md_match.group(2))
                    exec_date = date(y, m, d)
                except ValueError:
                    pass

        # --- 3. 数据提取 ---
        items = []
        seen_categories = set()

        def clean_category_name(raw_cat: str) -> str:
            s = re.sub(r'^\d+[、.．\s]+', '', raw_cat)
            s = re.sub(r'^[\. \-、,，.]*', '', s)
            s = s.replace('（', '(').replace('）', ')')
            return s.strip()

        def try_extract_from_text(text: str):
            if not text: return None
            match = re.search(r"^([\u4e00-\u9fa5A-Z0-9()、.]+?)\s*[:：]?\s*(\d{4,})", text)

            if not match:
                match = re.search(r"^([\u4e00-\u9fa5A-Z0-9()、.\-]+?)(\d{4,})", text)

            if not match:
                return None

            raw_cat = match.group(1).strip()
            price_str = match.group(2)

            if any(k in raw_cat for k in ["种类", "报价", "涨跌", "单位", "价格条件", "废铅", "回收", "序号", "提示", "合同", "送货时限"]):
                return None

            if re.search(r"\d{4}[\.\-/]\d{1,2}[\.\-/]\d{1,2}", text):
                return None

            category = clean_category_name(raw_cat)
            category = category.replace("吨", "").strip()

            if len(category) < 2:
                return None
            if "元" in category or "价" in category:
                return None
            if re.match(r"^\d+[\.\-/]*\d*[\.\-/]*\d*$", category):
                return None
            if "有限公司" in category:
                return None
            if re.match(r"^\d+[、.．]", category):
                return None
            if category in seen_categories:
                return None

            try:
                price = Decimal(price_str)
                if 1000 < price < 20000:
                    seen_categories.add(category)
                    return BatteryQuoteItem(category=category, price=price)
            except:
                pass
            return None

        # === 第一步：行重组 ===
        if text_lines:
            cleaned_items = []
            for line in text_lines:
                txt = line.get("text", "").strip()
                bbox = line.get("bbox")
                if not bbox: continue
                clean_txt = re.sub(r"[^\u4e00-\u9fa50-9.\-()AGM、：:]", "", txt)
                if clean_txt:
                    cleaned_items.append({
                        "text": clean_txt,
                        "bbox": bbox,
                        "y_center": (bbox[1] + bbox[3]) / 2
                    })

            if cleaned_items:
                cleaned_items.sort(key=lambda x: x["y_center"])
                grouped_rows = []
                current_row_items = []
                last_y = -1000

                for item in cleaned_items:
                    y = item["y_center"]
                    if abs(y - last_y) < 15:
                        current_row_items.append(item)
                    else:
                        if current_row_items:
                            current_row_items.sort(key=lambda x: x["bbox"][0])
                            grouped_rows.append("".join([x["text"] for x in current_row_items]))
                        current_row_items = [item]
                        last_y = y
                if current_row_items:
                    current_row_items.sort(key=lambda x: x["bbox"][0])
                    grouped_rows.append("".join([x["text"] for x in current_row_items]))

                for row in grouped_rows:
                    item = try_extract_from_text(row)
                    if item:
                        items.append(item)

                # === 额外处理：跨行品类名称合并 ===
                merged_rows = []
                i = 0
                while i < len(grouped_rows):
                    row = grouped_rows[i]
                    open_count = row.count('(') + row.count('（')
                    close_count = row.count(')') + row.count('）')
                    if open_count > close_count and i + 1 < len(grouped_rows):
                        merged_row = row + grouped_rows[i + 1]
                        item = try_extract_from_text(merged_row)
                        if item:
                            items.append(item)
                        i += 2
                    else:
                        i += 1

        # === 第二步：碎片扫描 ===
        if len(items) < 5:
            logger.info(f"⚠️ 重组行仅提取 {len(items)} 条，启动【碎片扫描】模式...")
            pending_category = None
            for line in text_lines:
                txt = line.get("text", "").strip()
                clean_txt = re.sub(r"[^\u4e00-\u9fa50-9.\-()AGM、：:]", "", txt)
                if not clean_txt: continue

                if pending_category:
                    clean_txt = pending_category + clean_txt
                    pending_category = None

                item = try_extract_from_text(clean_txt)
                if item:
                    cat = item.category
                    open_count = cat.count('(') + cat.count('（')
                    close_count = cat.count(')') + cat.count('）')
                    if open_count > close_count:
                        pending_category = cat
                    else:
                        items.append(item)
                else:
                    if re.search(r'[\u4e00-\u9fa5AGM]', clean_txt) and not re.search(r'\d{4,}', clean_txt):
                        pending_category = clean_txt

        # === 第三步：跨行合并修复 ===
        merged_items = []
        skip_next = False
        for i, item in enumerate(items):
            if skip_next:
                skip_next = False
                continue
            if item is None:
                continue
            cat = item.category

            need_merge = False
            open_count = cat.count('(') + cat.count('（')
            close_count = cat.count(')') + cat.count('）')

            if open_count > close_count:
                need_merge = True
            elif cat.endswith('、') or cat.endswith(','):
                need_merge = True

            if need_merge and i + 1 < len(items) and items[i + 1] is not None:
                next_item = items[i + 1]
                next_cat = next_item.category
                merged_cat = cat + next_cat
                merged_items.append(BatteryQuoteItem(category=merged_cat, price=next_item.price))
                skip_next = True
            else:
                merged_items.append(item)

        if merged_items:
            items = [item for item in merged_items if item is not None]

        return {
            "factory_name": factory_name,
            "exec_date": exec_date.strftime("%Y-%m-%d") if exec_date else None,
            "quote_type": "table_ultimate",
            "items": [item.to_dict() for item in items],
            "raw_text": full_text
        }
    def _parse_chat_style(self, text_lines: List[Dict], full_text: str) -> Dict[str, Any]:
        """
        【增强版】聊天截图解析器
        """
        factory_name = "未知工厂"
        exec_date = None

        match_factory = re.search(r"([\u4e00-\u9fa5]{4,}有限公司)", full_text)
        if match_factory:
            factory_name = match_factory.group(1)
        else:
            match_partial = re.search(r"([\u4e00-\u9fa5]{4,}有限公?司?)", full_text)
            if match_partial:
                partial_name = match_partial.group(1)
                if partial_name.endswith("有限公"):
                    factory_name = partial_name + "司"
                else:
                    factory_name = partial_name

        date_match = re.search(r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日\.?", full_text)
        if date_match:
            try:
                y, m, d = int(date_match.group(1)), int(date_match.group(2)), int(date_match.group(3))
                exec_date = date(y, m, d)
            except ValueError:
                pass

        if not exec_date:
            md_match = re.search(r"(\d{1,2})月\s*(\d{1,2})[号日]", full_text)
            if md_match:
                try:
                    y = datetime.now().year
                    m, d = int(md_match.group(1)), int(md_match.group(2))
                    exec_date = date(y, m, d)
                except ValueError:
                    pass

        items = []
        seen_categories = set()

        def clean_category_name(raw_cat: str) -> str:
            s = re.sub(r'^\d+[、.．\s]+', '', raw_cat)
            s = re.sub(r'^[\. \-、,，.]*', '', s)
            s = s.replace('（', '(').replace('）', ')')
            return s.strip().replace("吨", "")

        for line in text_lines:
            txt = line.get("text", "").strip()
            if not txt: continue

            if any(k in txt for k in ["种类", "价格", "序号", "送货", "提示", "要求", "合同", "截止", "登记"]):
                continue

            match = re.search(r"([\u4e00-\u9fa5A-Z0-9()、.]{2,})\s*[:：]?\s*(\d{4,})", txt)

            if match:
                raw_cat = match.group(1).strip()
                price_str = match.group(2)

                category = clean_category_name(raw_cat)

                if len(category) < 2 or "元" in category or "价" in category:
                    continue
                if category in seen_categories:
                    continue

                try:
                    price = Decimal(price_str)
                    if 1000 < price < 20000:
                        items.append(BatteryQuoteItem(category=category, price=price))
                        seen_categories.add(category)
                except:
                    continue

        return {
            "factory_name": factory_name,
            "exec_date": exec_date.strftime("%Y-%m-%d") if exec_date else None,
            "quote_type": "chat_enhanced",
            "items": [item.to_dict() for item in items],
            "raw_text": full_text
        }
    def _is_table_style(self, full_text: str) -> bool:
        """简单判断是否为表格风格"""
        if any(k in full_text for k in ["种类", "报价表", "单价", "元/吨"]):
            return True
        lines = full_text.split('\n')
        numbered_lines = 0
        for line in lines:
            if re.match(r"^\d+[、,.]", line.strip()):
                numbered_lines += 1
        if numbered_lines > len(lines) * 0.5:
            return False
        return True

    def parse_image(self, image_path: str) -> Dict[str, Any]:
        """主入口：解析图片"""
        if not os.path.exists(image_path):
            return {"error": "文件不存在"}

        import time
        start_time = time.time()

        # OCR 识别
        result, _ = self.ocr(image_path)
        ocr_time = (time.time() - start_time) * 1000
        logger.info(f"OCR 耗时：{ocr_time:.2f} ms")

        if not result:
            return {"error": "OCR 未识别到内容"}

        # 格式化 OCR 结果
        text_lines = []
        for line in result:
            box = line[0]
            text = line[1]
            xmin = min(p[0] for p in box)
            ymin = min(p[1] for p in box)
            xmax = max(p[0] for p in box)
            ymax = max(p[1] for p in box)

            text_lines.append({
                "text": text,
                "bbox": [xmin, ymin, xmax, ymax]
            })

        full_text = "\n".join([l["text"] for l in text_lines])

        # 检测类型并分发
        if len(text_lines) > 5:
            coord_result = self._parse_table_by_coords(text_lines, full_text)
            if coord_result and len(coord_result["items"]) > 0:
                logger.info("✅ 识别成功！(坐标布局分析)")
                return self._format_result(coord_result, time.time() - start_time)

        if self._is_table_style(full_text):
            logger.warning("未识别出明确格式，尝试混合解析 (表格正则)")
            table_result = self._parse_table_style(text_lines, full_text)
            if table_result["items"]:
                 logger.info("✅ 识别成功！(表格正则)")
                 return self._format_result(table_result, time.time() - start_time)

        logger.info("检测到聊天截图型报价单")
        chat_result = self._parse_chat_style(text_lines, full_text)
        logger.info("✅ 识别成功！")
        return self._format_result(chat_result, time.time() - start_time)

    def _format_result(self, data: Dict[str, Any], duration: float) -> Dict[str, Any]:
        return {
            "factory": data["factory_name"],
            "date": data["exec_date"],
            "type": data["quote_type"],
            "duration": f"{duration:.3f}s",
            "message": f"识别完成，共解析 {len(data['items'])} 条报价",
            "items": data["items"]
        }

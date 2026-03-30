#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VLM Extractor 2.0 - 千问VLM全量报价表提取器 (单文件版)

API Key 设置方式（三选一）：
    1. 命令行参数: -k sk-xxxxxx
    2. 环境变量:    set QWEN_API_KEY=sk-xxxxxx
    3. .env 文件:   创建 .env 文件写入 QWEN_API_KEY=sk-xxxxxx

使用方式：
    单文件:   python vlm_extractor_service.py 12.jpg -k sk-xxxxxx
    批量:     python vlm_extractor_service.py "./pics/*.jpg" -k sk-xxxxxx
    目录:     python vlm_extractor_service.py ./images -k sk-xxxxxx -r
"""

import os
import re
import json
import base64
import time
import glob
import tempfile
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional, Callable, Tuple, Union
from dataclasses import dataclass, field, asdict

try:
    from openai import OpenAI
except ImportError:
    raise ImportError("请安装OpenAI SDK: pip install openai>=1.0.0")

try:
    from pydantic import BaseModel, Field
except ImportError:
    raise ImportError("请安装Pydantic: pip install pydantic>=2.0.0")

try:
    from loguru import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import typer
    app = typer.Typer(add_completion=False)
except ImportError:
    typer = None
    app = None

try:
    from fastapi import FastAPI, File, UploadFile, HTTPException, Form
    import uvicorn
except ImportError:
    FastAPI = None
    uvicorn = None

# ==================== 数据模型 ====================

class PriceRow(BaseModel):
    """完整的一行数据（支持跨行品类继承）"""
    index: Optional[int] = None
    category: str = ""
    is_category_start: bool = False
    price_1pct_vat: Optional[int] = None
    price_3pct_vat: Optional[int] = None
    price_13pct_vat: Optional[int] = None
    price_normal_invoice: Optional[int] = None
    price_reverse_invoice: Optional[int] = None
    price_general: Optional[int] = None
    unit: str = "元/吨"
    remark: str = ""
    raw_text: str = ""


class PriceTableFull(BaseModel):
    """全量报价表数据"""
    image_path: str
    file_name: str
    success: bool
    company_name: str = ""
    doc_title: str = ""
    subtitle: str = ""
    quote_date: str = ""
    execution_date: str = ""
    valid_period: str = ""
    price_unit: str = "元/吨"
    headers: List[str] = Field(default_factory=list)
    rows: List[PriceRow] = Field(default_factory=list)
    policies: Dict[str, Any] = Field(default_factory=dict)
    footer_notes: List[str] = Field(default_factory=list)
    footer_notes_raw: str = ""
    brand_specifications: str = ""
    raw_full_text: str = ""
    markdown_table: str = ""
    vat_columns_detected: List[str] = Field(default_factory=list)
    has_merged_cells: bool = False
    price_column_type: str = "unknown"
    elapsed_time: float = 0.0
    error_message: Optional[str] = None
    output_path: Optional[str] = None
    
    def save(self, output_path: str) -> str:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(self.model_dump_json(indent=2, ensure_ascii=False))
        return str(path)


class BatchSummary(BaseModel):
    """批量处理汇总"""
    total_files: int
    successful: int
    failed: int
    processed_at: str
    results: List[PriceTableFull]
    
    def save(self, output_path: str):
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(self.model_dump(), f, ensure_ascii=False, indent=2)


# ==================== 配置类 ====================

class VLMConfig(BaseModel):
    """VLM配置"""
    api_key: Optional[str] = None
    model: str = "qwen-vl-max-latest"
    base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    output_dir: str = "./vlm_full_results"
    save_individual: bool = True
    save_summary: bool = False
    recursive: bool = False
    supported_ext: Tuple[str, ...] = ('.jpg', '.jpeg', '.png', '.bmp', '.tiff', '.webp', '.gif')
    temperature: float = 0.1
    max_tokens: int = 8192
    on_progress: Optional[Callable] = Field(default=None, exclude=True)
    on_error: Optional[Callable] = Field(default=None, exclude=True)
    on_complete: Optional[Callable] = Field(default=None, exclude=True)
    
    def model_post_init(self, __context) -> None:
        if not self.api_key:
            self.api_key = os.getenv("QWEN_API_KEY") or os.getenv("DASHSCOPE_API_KEY")
        
        if not self.api_key:
            raise ValueError(
                f"未设置API Key。请通过以下方式之一提供：\n"
                f"1. 命令行参数: -k sk-xxxxxx\n"
                f"2. 环境变量:   set QWEN_API_KEY=sk-xxxxxx\n"
                f"3. .env文件:   创建.env文件，写入 QWEN_API_KEY=sk-xxxxxx\n"
                f"当前工作目录: {Path.cwd()}"
            )


# ==================== 核心提取器 ====================

FULL_EXTRACTION_PROMPT = """你是一个专业的废铅酸蓄电池报价表OCR助手。请提取图片中的表格数据。

【重要规则】
1. **跨行品类处理**：如果"品类/电池名称"列存在合并单元格（如一个品类名对应多行价格），请为每一行都填充该品类名，并用 is_category_start 标记首行。
2. **序号处理**：保留原始序号，截图可能从中间开始（如从5开始），不要强制重排。
3. **价格列识别**：
   - 通辽泰鼎型：只有"单价（元/吨）"一列 -> 填入 price_general
   - 宁夏新益威型：有"含1%普票"和"含3%专票" -> 分别填入 price_1pct_vat 和 price_3pct_vat
   - 安徽天畅型：有"含3%专票"和"含13%专票" -> 分别填入 price_3pct_vat 和 price_13pct_vat
4. **长文本处理**："质检标准"列可能包含多行文本和特殊符号，请：
   - 将换行符替换为 \\n
   - 将双引号 " 替换为 '
   - 确保是合法的JSON字符串

【输出格式 - 严格JSON】
请直接输出JSON对象（不要Markdown代码块，不要```json标记），确保JSON语法100%正确：

{
  "company_name": "公司名称",
  "doc_title": "报价表",
  "execution_date": "2026年03月06日",
  "headers": ["电池名称", "单价（元/吨）含1%普票", "单价（元/吨）含3%专票", "质检标准"],
  "rows": [
    {
      "index": 1,
      "category": "电动车电池",
      "is_category_start": true,
      "price_1pct_vat": 9550,
      "price_3pct_vat": 9737,
      "price_13pct_vat": null,
      "price_normal_invoice": null,
      "price_reverse_invoice": null,
      "price_general": null,
      "remark": "...",
      "raw_text": "..."
    }
  ],
  "footer_notes": ["条款1", "条款2"],
  "footer_notes_raw": "...",
  "brand_specifications": "",
  "raw_full_text": "..."
}

【JSON语法检查清单】
- 所有字符串使用双引号，不要用单引号
- 数字不要用引号包裹
- 最后一项后面不要加逗号
- 换行符必须转义为 \\n
- null 值不要用引号"""


class QwenVLFullExtractor:
    def __init__(self, config: Optional[VLMConfig] = None):
        self.config = config or VLMConfig()
        self._client: Optional[Any] = None
        self._initialized = False

    def __enter__(self):
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()
        return False

    def initialize(self):
        if not self._initialized:
            self._client = OpenAI(
                api_key=self.config.api_key,
                base_url=self.config.base_url
            )
            self._initialized = True
            logger.info(f"VLM初始化: {self.config.model}")
        return self

    def cleanup(self):
        self._client = None
        self._initialized = False

    def _encode_image(self, image_path: str) -> str:
        with open(image_path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")

    def _normalize_path(self, path: str) -> str:
        return path.strip().replace('"', '').replace("'", "").replace('\\', '/')

    def _is_image(self, path: str) -> bool:
        return os.path.splitext(path)[1].lower() in self.config.supported_ext

    def _get_output_path(self, input_path: str) -> Path:
        return Path(self.config.output_dir) / f"{Path(input_path).stem}_full.json"

    def _parse_response(self, content: str) -> Dict[str, Any]:
        original_content = content.strip()
        json_block_patterns = [
            r'```json\s*([\s\S]*?)\s*```',
            r'```\s*([\s\S]*?)\s*```'
        ]
        for pattern in json_block_patterns:
            blocks = re.findall(pattern, original_content)
            for block in blocks:
                try:
                    cleaned = self._clean_json_string(block.strip())
                    return json.loads(cleaned)
                except json.JSONDecodeError:
                    continue
        
        try:
            start = original_content.find('{')
            if start == -1:
                raise ValueError("未找到JSON起始符 {")
            
            brace_count = 0
            end = -1
            for i, char in enumerate(original_content[start:], start):
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        end = i
                        break
            
            if brace_count != 0 or end == -1:
                last_brace = original_content.rfind('}')
                if last_brace != -1 and last_brace > start:
                    json_str = original_content[start:last_brace+1]
                else:
                    raise ValueError("无法找到完整的JSON对象")
            else:
                json_str = original_content[start:end+1]
            
            cleaned = self._clean_json_string(json_str)
            return json.loads(cleaned)
            
        except Exception as e:
            debug_path = f"debug_vlm_error_{int(time.time())}.txt"
            try:
                with open(debug_path, 'w', encoding='utf-8') as f:
                    f.write(f"Error: {str(e)}\n\nOriginal content:\n{original_content}")
                logger.error(f"调试文件已保存: {debug_path}")
            except:
                pass
            raise ValueError(f"JSON解析失败: {e}")

    def _clean_json_string(self, json_str: str) -> str:
        json_str = re.sub(r'//.*?$', '', json_str, flags=re.MULTILINE)
        json_str = re.sub(r'/\*.*?\*/', '', json_str, flags=re.DOTALL)
        json_str = re.sub(r',\s*}', '}', json_str)
        json_str = re.sub(r',\s*]', ']', json_str)
        json_str = json_str.replace('\\\\', '\x00ESCAPED_BACKSLASH\x00')
        
        def escape_in_string(match):
            s = match.group(0)
            s = s.replace('\n', '\\n').replace('\t', '\\t').replace('\r', '')
            return s
        
        json_str = re.sub(r'"[^"\\]*(?:\\.[^"\\]*)*"', escape_in_string, json_str)
        json_str = json_str.replace('\x00ESCAPED_BACKSLASH\x00', '\\\\')
        json_str = json_str.lstrip('\ufeff').strip()
        return json_str

    def _fill_merged_categories(self, rows_data: List[Dict]) -> List[Dict]:
        if not rows_data:
            return []
        filled_rows = []
        last_category = ""
        for row in rows_data:
            category = str(row.get("category") or "").strip()
            if not category and last_category:
                row["category"] = last_category
                row["is_category_start"] = False
            elif category:
                last_category = category
                row["is_category_start"] = True
            else:
                row["is_category_start"] = False
            filled_rows.append(row)
        return filled_rows

    def _detect_price_column_type(self, headers: List[str], rows: List[Dict]) -> Tuple[str, List[str]]:
        header_text = " ".join(headers).lower()
        detected_vat = []
        if any(x in header_text for x in ["1%", "1普"]):
            detected_vat.append("1%")
        if any(x in header_text for x in ["3%", "3专"]):
            detected_vat.append("3%")
        if any(x in header_text for x in ["13%", "13专"]):
            detected_vat.append("13%")
        
        if len(detected_vat) == 0:
            if rows and any(r.get("price_general") for r in rows[:3]):
                return "single", detected_vat
            return "unknown", detected_vat
        elif "1%" in detected_vat and "3%" in detected_vat:
            return "1_3_percent", detected_vat
        elif "3%" in detected_vat and "13%" in detected_vat:
            return "3_13_percent", detected_vat
        elif "3%" in detected_vat:
            return "3_percent_only", detected_vat
        return "unknown", detected_vat

    def _safe_int(self, val) -> Optional[int]:
        if val is None or val == "":
            return None
        try:
            if isinstance(val, (int, float)):
                return int(val)
            if isinstance(val, str):
                val = val.replace(',', '').replace('，', '').replace(' ', '').strip()
                return int(float(val))
            return None
        except (ValueError, TypeError):
            return None

    def _process_single(self, image_path: str) -> PriceTableFull:
        if not self._initialized:
            raise RuntimeError("服务未初始化")
        
        image_path = self._normalize_path(image_path)
        abs_path = os.path.abspath(image_path)
        file_name = os.path.basename(image_path)
        start_time = time.time()
        
        if not os.path.exists(abs_path):
            return PriceTableFull(
                image_path=abs_path,
                file_name=file_name,
                success=False,
                error_message="文件不存在"
            )
        
        try:
            base64_image = self._encode_image(abs_path)
            logger.info(f"正在识别: {file_name}")
            
            response = self._client.chat.completions.create(
                model=self.config.model,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "text", "text": FULL_EXTRACTION_PROMPT},
                        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}
                    ]
                }],
                temperature=self.config.temperature,
                max_tokens=self.config.max_tokens
            )
            
            vlm_output = response.choices[0].message.content
            elapsed = round(time.time() - start_time, 2)
            
            try:
                data = self._parse_response(vlm_output)
            except Exception as json_err:
                logger.error(f"JSON解析失败: {json_err}")
                return PriceTableFull(
                    image_path=abs_path,
                    file_name=file_name,
                    success=False,
                    error_message=f"JSON解析失败: {json_err}",
                    raw_full_text=vlm_output[:2000],
                    elapsed_time=elapsed
                )
            
            raw_rows = data.get("rows", [])
            processed_rows = self._fill_merged_categories(raw_rows)
            headers = data.get("headers", [])
            price_type, vat_detected = self._detect_price_column_type(headers, processed_rows)
            has_merged = any(r.get("is_category_start") for r in processed_rows) if len(processed_rows) > 1 else False
            
            rows = []
            for row_data in processed_rows:
                rows.append(PriceRow(
                    index=self._safe_int(row_data.get("index")),
                    category=str(row_data.get("category") or ""),
                    is_category_start=bool(row_data.get("is_category_start", False)),
                    price_1pct_vat=self._safe_int(row_data.get("price_1pct_vat")),
                    price_3pct_vat=self._safe_int(row_data.get("price_3pct_vat")),
                    price_13pct_vat=self._safe_int(row_data.get("price_13pct_vat")),
                    price_normal_invoice=self._safe_int(row_data.get("price_normal_invoice")),
                    price_reverse_invoice=self._safe_int(row_data.get("price_reverse_invoice")),
                    price_general=self._safe_int(row_data.get("price_general")),
                    unit=str(row_data.get("unit") or "元/吨"),
                    remark=str(row_data.get("remark") or ""),
                    raw_text=str(row_data.get("raw_text") or "")
                ))
            
            result = PriceTableFull(
                image_path=abs_path,
                file_name=file_name,
                success=True,
                company_name=data.get("company_name", ""),
                doc_title=data.get("doc_title", ""),
                subtitle=data.get("subtitle", ""),
                quote_date=data.get("quote_date", ""),
                execution_date=data.get("execution_date", ""),
                valid_period=data.get("valid_period", ""),
                price_unit=data.get("price_unit", "元/吨"),
                headers=headers,
                rows=rows,
                policies=data.get("policies", {}),
                footer_notes=data.get("footer_notes", []),
                footer_notes_raw=data.get("footer_notes_raw", ""),
                brand_specifications=data.get("brand_specifications", ""),
                raw_full_text=data.get("raw_full_text", vlm_output[:5000]),
                markdown_table=data.get("markdown_table", ""),
                vat_columns_detected=vat_detected,
                has_merged_cells=has_merged,
                price_column_type=price_type,
                elapsed_time=elapsed
            )
            
            logger.info(f"提取成功: {file_name} ({len(rows)}行, 类型:{price_type}, 耗时:{elapsed}s)")
            return result
            
        except Exception as e:
            logger.error(f"提取失败 {file_name}: {e}")
            if self.config.on_error:
                self.config.on_error(abs_path, e)
            return PriceTableFull(
                image_path=abs_path,
                file_name=file_name,
                success=False,
                error_message=str(e),
                elapsed_time=round(time.time() - start_time, 2)
            )

    def recognize(self, image_path: str, save_output: Optional[bool] = None) -> PriceTableFull:
        result = self._process_single(image_path)
        should_save = self.config.save_individual if save_output is None else save_output
        if should_save and result.success:
            output_path = self._get_output_path(image_path)
            result.save(str(output_path))
            result.output_path = str(output_path)
            logger.info(f"已保存: {output_path}")
        if self.config.on_complete:
            self.config.on_complete(image_path, result)
        return result

    def recognize_batch(
        self,
        image_paths: List[str],
        save_individual: Optional[bool] = None,
        save_summary: Optional[bool] = None
    ) -> BatchSummary:
        total = len(image_paths)
        results = []
        success_count = 0
        do_save_individual = self.config.save_individual if save_individual is None else save_individual
        do_save_summary = self.config.save_summary if save_summary is None else save_summary
        
        logger.info(f"批量处理: {total}个文件")
        
        for i, path in enumerate(image_paths, 1):
            result = self.recognize(path, save_output=do_save_individual)
            results.append(result)
            if result.success:
                success_count += 1
            if self.config.on_progress:
                self.config.on_progress(i, total, result.file_name, result.output_path)
            status = "✓" if result.success else "✗"
            logger.info(f"[{i}/{total}] {status} {result.file_name}")
            if i < total:
                time.sleep(0.5)
        
        summary = BatchSummary(
            total_files=total,
            successful=success_count,
            failed=total - success_count,
            processed_at=datetime.now().isoformat(),
            results=results
        )
        
        if do_save_summary:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            summary_path = Path(self.config.output_dir) / f"batch_summary_{timestamp}.json"
            summary.save(str(summary_path))
            logger.info(f"汇总已保存: {summary_path}")
        
        return summary


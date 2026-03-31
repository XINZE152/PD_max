"""
Qwen-VL 报价单智能识别系统 - 封装版
支持：标准报价单识别 + 安徽天畅/鲁控特殊格式修复
"""

import os
import sys
import json
import base64
import requests
import time
import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Union, Callable
from dataclasses import dataclass, asdict, field
from datetime import datetime
import logging
import glob
from concurrent.futures import ThreadPoolExecutor, as_completed
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

# 可选依赖处理
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    
    class tqdm:
        def __init__(self, iterable=None, **kwargs):
            self.iterable = iterable
            self.n = 0
            self.total = kwargs.get('total', 0)
        def __iter__(self):
            for item in self.iterable:
                yield item
                self.n += 1
        def update(self, n=1): self.n += n
        def set_postfix(self, **kwargs): pass
        def close(self): pass


@dataclass
class QuoteItem:
    """报价单项数据类"""
    category: str
    price: Optional[float] = None
    reverse_price: Optional[float] = None
    price_1pct: Optional[float] = None  # 1%税率价格
    price_3pct: Optional[float] = None  # 3%税率价格  
    price_13pct: Optional[float] = None  # 13%税率价格
    row_span_category: bool = False  # 是否为跨行品类名
    
    def to_dict(self) -> Dict:
        return {
            "category": self.category,
            "price": self.price,
            "reverse_price": self.reverse_price,
            "1%_price": self.price_1pct,
            "3%_price": self.price_3pct,
            "13%_price": self.price_13pct,
            "is_span": self.row_span_category
        }


@dataclass
class QuoteResult:
    """识别结果数据类"""
    image_path: str
    file_name: str
    success: bool
    factory: str = ""
    date: str = ""
    items: List[QuoteItem] = field(default_factory=list)
    raw_response: str = ""
    elapsed_time: float = 0.0
    error_message: Optional[str] = None
    output_path: Optional[str] = None
    metadata: Dict = field(default_factory=dict)  # 额外元数据
    
    def to_dict(self) -> Dict:
        return {
            "image_path": self.image_path,
            "file_name": self.file_name,
            "success": self.success,
            "factory": self.factory,
            "date": self.date,
            "items": [item.to_dict() for item in self.items],
            "raw_response": self.raw_response,
            "elapsed_time": self.elapsed_time,
            "error_message": self.error_message,
            "output_path": self.output_path,
            "metadata": self.metadata
        }
    
    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=indent)
    
    def save(self, output_path: Union[str, Path]) -> str:
        """保存结果为JSON"""
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(self.to_json())
        self.output_path = str(path)
        return str(path)


class QuotePostProcessor(ABC):
    """报价单后处理器抽象基类"""
    
    @abstractmethod
    def can_process(self, result: QuoteResult) -> bool:
        """检查是否能处理此结果"""
        pass
    
    @abstractmethod
    def process(self, result: QuoteResult) -> QuoteResult:
        """处理并返回修复后的结果"""
        pass


class AnhuiSpecialPostProcessor(QuotePostProcessor):
    """
    安徽天畅/鲁控特殊格式修复器
    修复：1) 序号不以1开头 2) 跨行品类名合并 3) 税率列区分
    """
    
    TARGET_FACTORIES = ["安徽天畅", "安徽鲁控", "天畅", "鲁控"]
    
    def can_process(self, result: QuoteResult) -> bool:
        factory = result.factory or ""
        return any(target in factory for target in self.TARGET_FACTORIES)
    
    def process(self, result: QuoteResult) -> QuoteResult:
        if not result.items:
            return result
        
        logger.info(f"应用安徽特殊格式修复: {result.file_name}")
        
        items = result.items
        processed_items = []
        last_category = ""
        
        for i, item in enumerate(items):
            # 修复1: 序号不从1开始时，保持索引连续性
            # 修复2: 跨行品类名合并
            cat = item.category or ""
            
            # 检测是否为跨行品类名（当前品类名较短或包含数字，且下一个有具体型号）
            if self._is_partial_category(cat, last_category):
                # 合并品类名
                if last_category and not cat.startswith(last_category):
                    cat = f"{last_category}{cat}"
                item.row_span_category = True
            
            # 清理品类名中的序号前缀（如果原始不以1开头，我们也不强制添加）
            cat = self._clean_category(cat)
            item.category = cat
            last_category = cat
            
            # 修复3: 确保税率字段正确映射
            self._normalize_tax_fields(item)
            
            processed_items.append(item)
        
        result.items = processed_items
        result.metadata["post_processed"] = True
        result.metadata["processor"] = "AnhuiSpecialPostProcessor"
        return result
    
    def _is_partial_category(self, current: str, last: str) -> bool:
        """判断当前品类名是否为跨行部分"""
        if not current or not last:
            return False
        # 如果当前品类名很短（如"管式电池"）且上一个包含型号（如"120叉车"）
        if len(current) < 10 and len(last) < 15:
            if any(x in last for x in ["叉车", "电动车", "电池"]) and any(x in current for x in ["电池", "管式", "铅酸"]):
                return True
        return False
    
    def _clean_category(self, category: str) -> str:
        """清理品类名中的多余序号"""
        # 移除开头的序号如 "2.", "2、" 等，但保留原始编号逻辑
        cleaned = re.sub(r'^[\d\s\.\、]+', '', category)
        return cleaned.strip()
    
    def _normalize_tax_fields(self, item: QuoteItem):
        """标准化税率价格字段"""
        # 确保税率字段正确区分（1%/3% vs 3%/13%）
        # 如果模型返回的是字符串，尝试解析
        for field_name in ['price_1pct', 'price_3pct', 'price_13pct']:
            val = getattr(item, field_name)
            if isinstance(val, str):
                nums = re.findall(r'[\d.]+', val)
                setattr(item, field_name, float(nums[0]) if nums else None)


class QwenVLConfig:
    """Qwen-VL 配置类"""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "qwen-vl-plus",
        temperature: float = 0.1,
        max_tokens: int = 4096,
        timeout: int = 60,
        enable_post_processors: bool = True
    ):
        self.api_key = api_key or os.getenv('DASHSCOPE_API_KEY')
        if not self.api_key:
            raise ValueError("未提供 API Key")
        
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.timeout = timeout
        self.enable_post_processors = enable_post_processors
        
        # 默认Prompt（可自定义）
        self.prompt = """请分析这张报价单图片，提取以下信息并返回 JSON：
{
  "factory": "厂家名称（含有限公司）",
  "date": "日期（YYYY-MM-DD）",
  "items": [
    {
      "category": "品类名称",
      "price": null,
      "reverse_price": null,
      "1%_price": 数字或null,
      "3%_price": 数字或null,
      "13%_price": 数字或null
    }
  ]
}
注意：合并跨行品类名，如"120叉车"和"管式电池"要合并为"120叉车管式电池"。只返回JSON，不要其他文字。"""


class QuoteRecognitionEngine:
    """
    报价单识别引擎 - 主封装类
    集成：VLM识别 + 后处理 + 批量处理
    """
    
    def __init__(self, config: Optional[QwenVLConfig] = None):
        self.config = config or QwenVLConfig()
        self.url = "https://dashscope.aliyuncs.com/api/v1/services/aigc/multimodal-generation/generation"
        self.post_processors: List[QuotePostProcessor] = []
        
        if self.config.enable_post_processors:
            self._register_default_processors()
    
    def _register_default_processors(self):
        """注册默认后处理器"""
        self.post_processors.append(AnhuiSpecialPostProcessor())
    
    def register_processor(self, processor: QuotePostProcessor):
        """注册自定义后处理器"""
        self.post_processors.append(processor)
    
    def recognize(self, image_path: Union[str, Path]) -> QuoteResult:
        """
        识别单张报价单图片
        
        Args:
            image_path: 图片路径
            
        Returns:
            QuoteResult: 识别结果
        """
        abs_path = Path(image_path).resolve()
        file_name = abs_path.name
        
        if not abs_path.exists():
            return QuoteResult(
                image_path=str(abs_path),
                file_name=file_name,
                success=False,
                error_message="文件不存在"
            )
        
        try:
            start = time.time()
            result = self._call_api(abs_path)
            elapsed = time.time() - start
            
            # 转换为QuoteItem列表
            items = self._parse_items(result.get("items", []))
            
            quote_result = QuoteResult(
                image_path=str(abs_path),
                file_name=file_name,
                success=True,
                factory=result.get("factory", ""),
                date=result.get("date", ""),
                items=items,
                raw_response=json.dumps(result, ensure_ascii=False),
                elapsed_time=round(elapsed, 2)
            )
            
            # 应用后处理器
            quote_result = self._apply_post_processors(quote_result)
            
            return quote_result
            
        except Exception as e:
            logger.error(f"识别失败 [{file_name}]: {e}")
            return QuoteResult(
                image_path=str(abs_path),
                file_name=file_name,
                success=False,
                error_message=str(e)
            )
    
    def _call_api(self, image_path: Path) -> Dict:
        """调用DashScope API"""
        image_base64 = self._encode_image(image_path)
        
        headers = {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": self.config.model,
            "input": {
                "messages": [
                    {
                        "role": "system",
                        "content": [{"type": "text", "text": "You are a helpful assistant."}]
                    },
                    {
                        "role": "user",
                        "content": [
                            {"type": "image", "image": f"data:image/jpeg;base64,{image_base64}"},
                            {"type": "text", "text": self.config.prompt}
                        ]
                    }
                ]
            },
            "parameters": {
                "temperature": self.config.temperature,
                "max_tokens": self.config.max_tokens,
                "result_format": "message"
            }
        }
        
        response = requests.post(
            self.url, 
            headers=headers, 
            json=payload, 
            timeout=self.config.timeout
        )
        
        if response.status_code != 200:
            raise RuntimeError(f"API错误 {response.status_code}: {response.text}")
        
        result = response.json()
        content = result["output"]["choices"][0]["message"]["content"]
        
        if isinstance(content, list) and len(content) > 0:
            text = content[0].get("text", "")
        else:
            text = str(content)
        
        # 提取JSON
        json_match = re.search(r'\{.*\}', text, re.DOTALL)
        if json_match:
            return json.loads(json_match.group())
        return json.loads(text)
    
    def _encode_image(self, image_path: Path) -> str:
        """Base64编码图片"""
        with open(image_path, "rb") as f:
            return base64.b64encode(f.read()).decode('utf-8')
    
    def _parse_items(self, raw_items: List[Dict]) -> List[QuoteItem]:
        """解析原始项目列表"""
        items = []
        for raw in raw_items:
            item = QuoteItem(
                category=raw.get("category", ""),
                price=self._extract_number(raw.get("price")),
                reverse_price=self._extract_number(raw.get("reverse_price")),
                price_1pct=self._extract_number(raw.get("1%_price")),
                price_3pct=self._extract_number(raw.get("3%_price")),
                price_13pct=self._extract_number(raw.get("13%_price"))
            )
            items.append(item)
        return items
    
    def _extract_number(self, val) -> Optional[float]:
        """从字符串中提取数字"""
        if val is None or val == "":
            return None
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, str):
            nums = re.findall(r'[\d.]+', val)
            return float(nums[0]) if nums else None
        return None
    
    def _apply_post_processors(self, result: QuoteResult) -> QuoteResult:
        """应用所有适用的后处理器"""
        for processor in self.post_processors:
            if processor.can_process(result):
                result = processor.process(result)
        return result
    
    def batch_recognize(
        self,
        image_paths: List[Union[str, Path]],
        max_workers: int = 1,
        progress_callback: Optional[Callable[[str, bool], None]] = None,
        use_tqdm: bool = True
    ) -> List[QuoteResult]:
        """
        批量识别
        
        Args:
            image_paths: 图片路径列表
            max_workers: 并发数（建议2-4）
            progress_callback: 进度回调函数，参数为(filename, success)
            use_tqdm: 是否使用进度条
            
        Returns:
            List[QuoteResult]: 识别结果列表
        """
        results = []
        
        if max_workers > 1:
            # 并发处理
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_path = {
                    executor.submit(self.recognize, path): path 
                    for path in image_paths
                }
                
                iterator = as_completed(future_to_path)
                if use_tqdm and TQDM_AVAILABLE:
                    iterator = tqdm(iterator, total=len(image_paths), desc="识别中")
                
                for future in iterator:
                    result = future.result()
                    results.append(result)
                    if progress_callback:
                        progress_callback(result.file_name, result.success)
        else:
            # 单线程
            iterator = tqdm(image_paths, desc="识别中") if (use_tqdm and TQDM_AVAILABLE) else image_paths
            for path in iterator:
                result = self.recognize(path)
                results.append(result)
                if progress_callback:
                    progress_callback(result.file_name, result.success)
        
        return results


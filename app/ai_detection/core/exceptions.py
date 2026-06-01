"""异常分类体系 — 区分可恢复业务异常与不可恢复系统异常。"""


class DetectionError(Exception):
    """检测引擎异常基类。"""


class RecoverableError(DetectionError):
    """可恢复的业务异常：对用户返回友好提示。"""


class SystemDetectionError(DetectionError):
    """不可恢复的系统异常：需查看完整 traceback 定位根因。"""


class ImageReadError(RecoverableError):
    """图片无法读取或格式不支持。"""


class ModelInferenceError(SystemDetectionError):
    """模型推理过程中的非预期系统错误。"""

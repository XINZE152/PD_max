#!/bin/bash
# 规则检测性能诊断脚本（bash 版）
# 用法: bash diagnose_rule_check.sh <图片路径>
# 对比: 直连后端 vs Vite 代理

set -e

IMAGE="${1:-}"
if [ -z "$IMAGE" ] || [ ! -f "$IMAGE" ]; then
  echo "用法: bash diagnose_rule_check.sh <图片路径>"
  echo "示例: bash diagnose_rule_check.sh uploads/ai_detection_storage/xxx.jpg"
  exit 1
fi

BACKEND_URL="http://localhost:8002"
PROXY_URL="http://localhost:5173"
ENDPOINT="/ai-detection/api/v1/rule-checks"
FILE_SIZE=$(ls -lh "$IMAGE" | awk '{print $5}')

echo "============================================"
echo "  规则检测性能诊断"
echo "============================================"
echo "  图片: $IMAGE"
echo "  大小: $FILE_SIZE"
echo "  端点: $ENDPOINT"
echo "============================================"
echo ""

# 1. 检查后端是否存活
echo ">>> [1/5] 检查后端存活..."
if curl -s -o /dev/null -w "%{http_code}" "$BACKEND_URL/docs" | grep -q 200; then
  echo "  ✅ 后端 $BACKEND_URL 正常"
else
  echo "  ❌ 后端 $BACKEND_URL 不可达"
  exit 1
fi

# 2. 检查代理是否存活
echo ">>> [2/5] 检查代理存活..."
PROXY_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$PROXY_URL" 2>/dev/null || echo "000")
if [ "$PROXY_STATUS" = "200" ]; then
  echo "  ✅ 代理 $PROXY_URL 正常"
else
  echo "  ⚠️  代理 $PROXY_URL 返回 $PROXY_STATUS（可能未启动或端口不对）"
fi

# 3. 快速 GET 测试（直连 vs 代理）
echo ">>> [3/5] GET 快速测试..."
DIRECT_GET=$(curl -s -o /dev/null -w "%{time_total}" "$BACKEND_URL/docs")
PROXY_GET=$(curl -s -o /dev/null -w "%{time_total}" "$PROXY_URL/docs")
echo "  直连 GET /docs : ${DIRECT_GET}s"
echo "  代理 GET /docs : ${PROXY_GET}s"

# 4. 规则检测 POST 测试（直连）
echo ">>> [4/5] POST 规则检测 — 直连 $BACKEND_URL ..."
DIRECT_TIMES=""
for i in 1 2 3; do
  t=$(curl -s -o /dev/null -w "%{time_total}" -X POST "$BACKEND_URL$ENDPOINT" -F "file=@$IMAGE")
  DIRECT_TIMES="$DIRECT_TIMES $t"
  echo "  直连 第${i}次: ${t}s"
done

# 5. 规则检测 POST 测试（代理）
echo ">>> [5/5] POST 规则检测 — 代理 $PROXY_URL ..."
PROXY_TIMES=""
for i in 1 2 3; do
  t=$(curl -s -o /dev/null -w "%{time_total}" -X POST "$PROXY_URL$ENDPOINT" -F "file=@$IMAGE")
  PROXY_TIMES="$PROXY_TIMES $t"
  echo "  代理 第${i}次: ${t}s"
done

# 汇总
echo ""
echo "============================================"
echo "  汇总"
echo "============================================"

# 计算平均值（使用 awk 代替 bc，兼容 Windows Git Bash）
DIRECT_AVG=$(echo "$DIRECT_TIMES" | awk '{printf "%.2f", ($1+$2+$3)/3}')
PROXY_AVG=$(echo "$PROXY_TIMES" | awk '{printf "%.2f", ($1+$2+$3)/3}')
OVERHEAD=$(echo "$DIRECT_AVG $PROXY_AVG" | awk '{printf "%.2f", $2-$1}')
RATIO=$(echo "$DIRECT_AVG $PROXY_AVG" | awk '{printf "%.1f", $2/$1}')

echo "  直连平均耗时 : ${DIRECT_AVG}s"
echo "  代理平均耗时 : ${PROXY_AVG}s"
echo "  代理额外开销 : ${OVERHEAD}s"
echo "  倍率         : ${RATIO}x"

if [ "$(echo "$OVERHEAD" | awk '{print ($1 > 1.0 ? "1" : "0")}')" = "1" ]; then
  echo ""
  echo "  ⚠️  代理额外开销 > 1 秒，可能存在以下问题："
  echo "     1. .env 中 VITE_API_TARGET 可能包含 /docs 后缀"
  echo "     2. vite.config.ts 的 proxyTimeout/timeout 未生效（需重启 dev server）"
  echo "     3. Vite 代理对 multipart 上传的缓冲策略问题"
  echo ""
  echo "  🔧 排查步骤："
  echo "     a. 检查 frontend/.env：VITE_API_TARGET=http://localhost:8002（无 /docs 后缀）"
  echo "     b. 重启前端 dev server：npm run dev"
  echo "     c. 重新运行本脚本验证"
fi
echo "============================================"

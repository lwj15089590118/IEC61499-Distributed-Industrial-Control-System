# -*- coding: utf-8 -*-
"""
================================================================================
 simulator/order_generator.py —— 生产订单模拟器（MES/ERP 角色）
================================================================================
职责：随机生成生产订单（产品类型、数量、交付时间），发布 OrderReceived
事件到 factory/orders 主题，驱动整个分布式控制系统运转。

特性：
  - 产品类型按产线实际产能分布加权随机（走量产品概率高）；
  - 数量服从对数正态分布（小批量多品种、偶发大批量）；
  - 交付时间按数量与产线节拍估算，叠加随机提前期；
  - 支持 --count / --interval / --burst 参数灵活控制压测节奏；
  - 通过共享内存通道直发（无需 MQTT Broker 也能驱动单机部署）。

用法示例：
  python simulator/order_generator.py                     # 持续随机下单
  python simulator/order_generator.py --count 5           # 只下 5 单
  python simulator/order_generator.py --interval 3        # 每 3 秒一单
  python simulator/order_generator.py --burst 10          # 瞬时洪峰 10 单
================================================================================
"""

from __future__ import annotations

import argparse
import logging
import random
import sys
import time
from pathlib import Path
from typing import Dict, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from communication.message_types import (EventType, Priority, Topics,  # noqa: E402
                                         make_message)
from core.distributed_runtime import (RUNTIME_DIR, MQTTChannel,  # noqa: E402
                                      SharedMemoryChannel, configure_logger,
                                      load_config)

logger = logging.getLogger("order_gen")

# ==============================================================================
# 1. 产品目录（类型、权重、单位节拍秒/件、缺陷倾向）
# ==============================================================================

PRODUCT_CATALOG: Dict[str, Dict] = {
    "电机外壳":   {"weight": 0.40, "takt_s": 6.0,  "price": 45.0},
    "铝支架":     {"weight": 0.30, "takt_s": 4.5,  "price": 28.0},
    "传感器面板": {"weight": 0.20, "takt_s": 7.5,  "price": 65.0},
    "精密齿轮":   {"weight": 0.10, "takt_s": 10.0, "price": 120.0},
}

_ORDER_SEQ = 0                      # 订单流水号（进程内递增）


def next_order_id() -> str:
    """生成全局唯一订单号：ORD-日期-序号。"""
    global _ORDER_SEQ
    _ORDER_SEQ += 1
    return "ORD-%s-%04d" % (time.strftime("%Y%m%d"), _ORDER_SEQ)


# ==============================================================================
# 2. 订单生成逻辑
# ==============================================================================


def pick_product() -> str:
    """按产能权重随机挑选产品类型。"""
    products = list(PRODUCT_CATALOG)
    weights = [PRODUCT_CATALOG[p]["weight"] for p in products]
    return random.choices(products, weights=weights, k=1)[0]


def pick_quantity() -> int:
    """
    生成订单数量：对数正态分布（多小单、少大单）。

    mu=2.2, sigma=0.6 时中位数约 9 件，95 分位约 40 件，贴合多品种
    小批量柔性产线的真实订单结构。
    """
    qty = int(random.lognormvariate(2.2, 0.6))
    return max(1, min(qty, 60))       # 夹紧到 [1, 60] 防极端值压垮队列


def estimate_deadline(product: str, quantity: int) -> float:
    """
    估算交付时间（秒，相对值）：
      工件节拍 x 数量 x 负荷系数 + 随机提前期(10~30% 余量)。
    """
    takt = PRODUCT_CATALOG[product]["takt_s"]
    base = takt * quantity * random.uniform(1.1, 1.3)
    return round(base + random.uniform(60.0, 300.0), 1)


def build_order(forced_product: Optional[str] = None) -> Dict:
    """组装一张完整订单报文（OrderReceived 事件的 payload）。"""
    product = forced_product or pick_product()
    quantity = pick_quantity()
    return {
        "order_id": next_order_id(),
        "product": product,
        "quantity": quantity,
        "deadline": estimate_deadline(product, quantity),
        "priority": random.choice([2, 2, 2, 1, 3]),   # 常规/加急/缓单
        "source": "simulator",
        "created_at": round(time.time(), 3),
        "unit_price": PRODUCT_CATALOG[product]["price"],
    }


# ==============================================================================
# 3. 发布通道（复用运行时的双通道实现：共享内存必达 + MQTT 可选）
# ==============================================================================


class OrderPublisher:
    """
    订单发布器：把 OrderReceived 事件写入共享事件流（并尝试 MQTT）。

    不依赖任何节点进程存活 —— 模拟器可以独立启动压测。
    """

    def __init__(self, config_path: Optional[str] = None) -> None:
        cfg = load_config(config_path)
        self.runtime_dir = RUNTIME_DIR
        mqtt_cfg = cfg.get("mqtt", {})
        # MQTT 通道：broker 不在线时自动降级，不影响共享内存直发
        self.mqtt = MQTTChannel("simulator",
                                mqtt_cfg.get("host", "127.0.0.1"),
                                int(mqtt_cfg.get("port", 1883)),
                                on_message=lambda m: None)
        if mqtt_cfg.get("enabled", True):
            self.mqtt.start([Topics.ORDERS])
        self.shm = SharedMemoryChannel("simulator", self.runtime_dir,
                                       on_message=lambda m: None)

    def publish_order(self, order: Dict) -> None:
        """发布一条订单事件（OrderReceived，HIGH 优先级）。"""
        msg = make_message(EventType.ORDER_RECEIVED, "simulator",
                           Topics.ORDERS, order,
                           priority=Priority.HIGH)
        if not self.shm.send(msg):                    # 共享内存通道必达
            logger.error("订单写入共享事件流失败！")
        else:
            logger.info("订单已发布：%s %s x%d（交期 %.0fs，优先级 P%d）",
                        order["order_id"], order["product"], order["quantity"],
                        order["deadline"], order["priority"])
        self.mqtt.send(msg)                           # MQTT 尽力而为

    def close(self) -> None:
        """释放通道。"""
        self.mqtt.stop()
        self.shm.stop()


# ==============================================================================
# 4. 命令行入口
# ==============================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description="生产订单模拟器：随机生成订单驱动分布式控制系统")
    parser.add_argument("--count", type=int, default=0,
                        help="订单总数（0 表示无限持续）")
    parser.add_argument("--interval", type=float, default=5.0,
                        help="订单间隔秒数（实际叠加 ±40% 抖动）")
    parser.add_argument("--burst", type=int, default=0,
                        help="启动时瞬时下发 N 单（洪峰测试，默认0）")
    parser.add_argument("--product", type=str, default=None,
                        help="强制指定产品类型（压测单一工艺路线）")
    parser.add_argument("--config", type=str, default=None,
                        help="nodes.yaml 路径（默认 core/nodes.yaml）")
    args = parser.parse_args()

    configure_logger("order_gen")
    publisher = OrderPublisher(args.config)
    logger.info("订单模拟器启动：interval=%.1fs count=%s product=%s",
                args.interval, args.count or "∞", args.product or "随机")

    try:
        # 洪峰模式：一次性灌入 burst 单
        for _ in range(max(0, args.burst)):
            publisher.publish_order(build_order(args.product))

        published = 0
        while args.count == 0 or published < args.count:
            publisher.publish_order(build_order(args.product))
            published += 1
            if args.count == 0 or published < args.count:
                # 抖动间隔：避免周期性负载掩盖调度器的优先级效果
                time.sleep(args.interval * random.uniform(0.6, 1.4))
        logger.info("订单模拟完成，共发布 %d 单。", published)
    except KeyboardInterrupt:
        logger.info("收到 Ctrl+C，订单模拟器退出。")
    finally:
        publisher.close()


if __name__ == "__main__":
    main()
